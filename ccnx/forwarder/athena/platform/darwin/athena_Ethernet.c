/*
 * Copyright (c) 2015, Xerox Corporation (Xerox)and Palo Alto Research Center (PARC)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Patent rights are not granted under this agreement. Patent rights are
 *       available under FRAND terms.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL XEROX or PARC BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/**
 * @author Kevin Fox, Palo Alto Research Center (Xerox PARC)
 * @copyright 2015, Xerox Corporation (Xerox)and Palo Alto Research Center (PARC).  All rights reserved.
 */
#include <config.h>

#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <net/if_dl.h>
#include <net/ethernet.h>
#include <net/bpf.h>
#include <ifaddrs.h>

#include <LongBow/runtime.h>
#include <parc/algol/parc_Object.h>
#include <parc/algol/parc_Memory.h>
#include <ccnx/forwarder/athena/athena_TransportLink.h>
#include <ccnx/forwarder/athena/athena_Ethernet.h>

struct AthenaEthernet {
    int fd;
    struct ether_addr mac;
    uint16_t etherType;
    uint32_t mtu;
    int etherBufferLength;
    PARCBuffer *bpfBuffer;
    PARCLog *log;
    ssize_t readCount;
};

static void
_athenaEthernet_Destroy(AthenaEthernet **athenaEthernet)
{
    parcLog_Release(&((*athenaEthernet)->log));
    close((*athenaEthernet)->fd);
    if ((*athenaEthernet)->bpfBuffer) {
        parcBuffer_Release(&((*athenaEthernet)->bpfBuffer));
    }
}

parcObject_ExtendPARCObject(AthenaEthernet, _athenaEthernet_Destroy, NULL, NULL, NULL, NULL, NULL, NULL);

parcObject_ImplementAcquire(athenaEthernet, AthenaEthernet);

parcObject_ImplementRelease(athenaEthernet, AthenaEthernet);

static int
_open_bpf_device(void)
{
    int etherSocket = -1;

    // Find the first available bpf device
    for (int i = 0; i < 255; i++) {
        char bpfstr[255];
        snprintf(bpfstr, sizeof(bpfstr), "/dev/bpf%d", i);

        etherSocket = open(bpfstr, O_RDWR);
        if (etherSocket == -1) {
            if ((errno == EBUSY) || (errno == EBADF)) {
                continue;
            }
        }
        break;
    }
    return etherSocket;
}

static int
_open_socket(const char *device)
{
    int etherSocket;

    etherSocket = _open_bpf_device();
    if (etherSocket == -1) {
        perror("bpf");
        return -1;
    }

    // Attach the requested interface
    struct ifreq if_idx = { {0} };
    strncpy(if_idx.ifr_name, device, strlen(device) + 1);
    if (ioctl(etherSocket, BIOCSETIF, &if_idx)) {
        perror("BIOCSETIF");
        return -1;
    }

    // Set immediate read on packet reception
    uint32_t on = 1;
    if (ioctl(etherSocket, BIOCIMMEDIATE, &on)) {
        perror("BIOCIMMEDIATE");
        return -1;
    }

    // Setup the BPF filter for CCNX_ETHERTYPE
    struct bpf_program filterCode = { 0 };

    // BPF instructions:
    // Load 12 to accumulator (offset of ethertype)
    // Jump Equal to netbyteorder_ethertype 0 instructions, otherwise 1 instruction
    // 0: return a length of -1 (meaning whole packet)
    // 1: return a length of 0 (meaning skip packet)
    struct bpf_insn instructions[] = {
        BPF_STMT(BPF_LD + BPF_H + BPF_ABS,  12),
        BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, CCNX_ETHERTYPE,0,  1),
        BPF_STMT(BPF_RET + BPF_K,           (u_int) - 1),
        BPF_STMT(BPF_RET + BPF_K,           0),
    };

    // Install the filter
    filterCode.bf_len = sizeof(instructions) / sizeof(struct bpf_insn);
    filterCode.bf_insns = &instructions[0];
    if (ioctl(etherSocket, BIOCSETF, &filterCode) < 0) {
        perror("BIOCSETF");
        return -1;
    }

    return etherSocket;
}

AthenaEthernet *
athenaEthernet_Create(PARCLog *log, const char *interface, uint16_t etherType)
{
    AthenaEthernet *athenaEthernet = parcObject_CreateAndClearInstance(AthenaEthernet);
    athenaEthernet->log = parcLog_Acquire(log);
    athenaEthernet->etherType = etherType;

    athenaEthernet->fd = _open_socket(interface);
    if (athenaEthernet->fd == -1) {
        parcLog_Error(athenaEthernet->log, "socket: %s", strerror(errno));

        athenaEthernet_Release(&athenaEthernet);
        return NULL;
    }

    if (ioctl(athenaEthernet->fd, BIOCGBLEN, &(athenaEthernet->etherBufferLength))) {
        perror("error getting buffer length");
        return NULL;
    }

    // Populate the configured physical MAC and MTU by searching ifaddrs
    struct ifaddrs *ifaddr;
    int res = getifaddrs(&ifaddr);
    if (res == -1) {
        perror("getifaddrs");
        return 0;
    }

    struct ifaddrs *next;
    for (next = ifaddr; next != NULL; next = next->ifa_next) {
        if (strcmp(next->ifa_name, interface) == 0) {
            if (next->ifa_addr->sa_family == AF_LINK) {
                struct sockaddr_dl *addr_dl = (struct sockaddr_dl *) next->ifa_addr;

                // addr_dl->sdl_data[12] contains the interface name followed by the MAC address, so
                // need to offset in to the array past the interface name.
                memcpy(&(athenaEthernet->mac), &addr_dl->sdl_data[addr_dl->sdl_nlen], addr_dl->sdl_alen);

                struct if_data *ifdata = (struct if_data *) next->ifa_data;
                athenaEthernet->mtu = ifdata->ifi_mtu;

                // break out of loop and freeifaddrs
                break;
            }
        }
    }
    freeifaddrs(ifaddr);

    return athenaEthernet;
}

uint32_t
athenaEthernet_GetMTU(AthenaEthernet *athenaEthernet)
{
    return athenaEthernet->mtu;
}

void
athenaEthernet_GetMAC(AthenaEthernet *athenaEthernet, struct ether_addr *ether_addr)
{
    int i;
    for (i = 0; i < ETHER_ADDR_LEN; i++) {
        ether_addr->ether_addr_octet[i] = athenaEthernet->mac.ether_addr_octet[i];
    }
}

int
athenaEthernet_GetInterfaceMAC(const char *device, struct ether_addr *ether_addr)
{
    int result = -1;

    // Lookup the MAC address of an interface that is up, then ask for it.  Don't use loopback.
    struct ifaddrs *ifaddr;
    int failure = getifaddrs(&ifaddr);
    assertFalse(failure, "Error getifaddrs: (%d) %s", errno, strerror(errno));

    struct ifaddrs *next;
    for (next = ifaddr; next != NULL; next = next->ifa_next) {
        if (strcmp(next->ifa_name, device) == 0) {
            if (next->ifa_addr->sa_family == AF_LINK) {
                struct sockaddr_dl *addr_dl = (struct sockaddr_dl *) next->ifa_addr;

                memcpy(ether_addr, &addr_dl->sdl_data[addr_dl->sdl_nlen], addr_dl->sdl_alen);
                result = 0;
                // break out of loop and freeifaddrs
                break;
            }
        }
    }
    freeifaddrs(ifaddr);
    return result;
}

uint16_t
athenaEthernet_GetEtherType(AthenaEthernet *athenaEthernet)
{
    return athenaEthernet->etherType;
}

PARCBuffer *
athenaEthernet_Receive(AthenaEthernet *athenaEthernet, int timeout, AthenaTransportLinkEvent *events)
{
    // Allocate, and read, a new BPF buffer if no packets are currently pending in an old one
    if (athenaEthernet->bpfBuffer == NULL) {
        athenaEthernet->bpfBuffer = parcBuffer_Allocate(athenaEthernet->etherBufferLength);
        uint8_t *buffer = parcBuffer_Overlay(athenaEthernet->bpfBuffer, 0);

        athenaEthernet->readCount = read(athenaEthernet->fd, buffer, athenaEthernet->etherBufferLength);
        if (athenaEthernet->readCount == -1) {
            parcLog_Error(athenaEthernet->log, "recv: %s\n", strerror(errno));
            parcBuffer_Release(&athenaEthernet->bpfBuffer);
            return NULL;
        }
        parcLog_Debug(athenaEthernet->log, "received bpf packet (size=%d)", athenaEthernet->readCount);
    }

    // Obtain the current position in the BPF buffer to return a message from
    size_t position = parcBuffer_Position(athenaEthernet->bpfBuffer);

    // Read the BPF header and seek past it
    struct bpf_hdr *bpfhdr = parcBuffer_Overlay(athenaEthernet->bpfBuffer, sizeof(struct bpf_hdr));
    parcBuffer_SetLimit(athenaEthernet->bpfBuffer, position + bpfhdr->bh_hdrlen + bpfhdr->bh_datalen);
    parcBuffer_SetPosition(athenaEthernet->bpfBuffer, position + bpfhdr->bh_hdrlen);
    parcLog_Debug(athenaEthernet->log, "received message (size=%d)", bpfhdr->bh_datalen);

    // Slice a new PARCBuffer with the message to send up.
    PARCBuffer *wireFormatBuffer = parcBuffer_Slice(athenaEthernet->bpfBuffer);

    // If there's another packet in the buffer, position it and flag a receive event
    if ((athenaEthernet->readCount - (position + bpfhdr->bh_hdrlen + bpfhdr->bh_datalen)) != 0) {
        parcBuffer_SetLimit(athenaEthernet->bpfBuffer, athenaEthernet->readCount);
        parcBuffer_SetPosition(athenaEthernet->bpfBuffer,
                               BPF_WORDALIGN(position + bpfhdr->bh_hdrlen + bpfhdr->bh_datalen));
        // Mark a receive event for this packet
        *events = AthenaTransportLinkEvent_Receive;
    } else {
        parcBuffer_Release(&athenaEthernet->bpfBuffer);
    }

    return wireFormatBuffer;
}

ssize_t
athenaEthernet_Send(AthenaEthernet *athenaEthernet, struct iovec *iov, int iovcnt)
{
    ssize_t writeCount;

    writeCount = writev(athenaEthernet->fd, iov, iovcnt);

    parcLog_Debug(athenaEthernet->log, "sending message (size=%d)", writeCount);

    return writeCount;
}

int
athenaEthernet_GetDescriptor(AthenaEthernet *athenaEthernet)
{
    return athenaEthernet->fd;
}
