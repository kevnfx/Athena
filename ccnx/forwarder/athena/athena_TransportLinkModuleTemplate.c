/*
 * Copyright (c) 2016, Xerox Corporation (Xerox)and Palo Alto Research Center (PARC)
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
 * @copyright 2016, Xerox Corporation (Xerox)and Palo Alto Research Center (PARC).  All rights reserved.
 */
#include <config.h>

#include <LongBow/runtime.h>

#include <errno.h>
#include <netdb.h>
#include <sys/param.h>
#include <arpa/inet.h>

#include <parc/algol/parc_Network.h>
#include <ccnx/forwarder/athena/athena_TransportLinkModule.h>

#include <ccnx/common/codec/ccnxCodec_TlvPacket.h>
#include <ccnx/common/ccnx_WireFormatMessage.h>

//
// Private data for each link instance
//
typedef struct _TemplateLinkData {
    struct {
        size_t receive_ReadHeaderFailure;
        size_t receive_BadMessageLength;
        size_t receive_ReadError;
        size_t receive_ReadWouldBlock;
        size_t receive_ShortRead;
        size_t receive_ShortWrite;
        size_t receive_DecodeFailed;
    } _stats;
} _TemplateLinkData;

static _TemplateLinkData *
_TemplateLinkData_Create()
{
    _TemplateLinkData *linkData = parcMemory_AllocateAndClear(sizeof(_TemplateLinkData));
    assertNotNull(linkData, "Could not create private data for new link");
    return linkData;
}

static void
_TemplateLinkData_Destroy(_TemplateLinkData **linkData)
{
    parcMemory_Deallocate(linkData);
}

/**
 * @abstract create unique link name based on private link information
 * @discussion
 *
 * @param [in] type of connection
 * @param [in] fd file descriptor
 * @return allocated name, must be released with parcMemory_Deallocate()
 *
 * Example:
 * @code
 * {
 *     char *linkName = _createNameFromLinkData(linkData);
 *     parcMemory_Deallocate(&linkName);
 * }
 * @endcode
 */
static const char *
_createNameFromLinkData(const _TemplateLinkData *linkData)
{
    char nameBuffer[MAXPATHLEN];
    const char *protocol = "Template";

    sprintf(nameBuffer, "%s://Unknown", protocol);

    return parcMemory_StringDuplicate(nameBuffer, strlen(nameBuffer));
}

static PARCBuffer
_ioVecToBuffer(CCNxCodecNetworkBufferIoVec *iovec)
{
    PARCBuffer wireFormatBuffer = NULL;
    size_t iovcnt = ccnxCodecNetworkBufferIoVec_GetCount((CCNxCodecNetworkBufferIoVec *) iovec);
    const struct iovec *array = ccnxCodecNetworkBufferIoVec_GetArray((CCNxCodecNetworkBufferIoVec *) iovec);

    // If it's a single vector wrap it in a buffer to avoid a copy
    if (iovcnt == 1) {
        wireFormatBuffer = parcBuffer_Wrap(array[0].iov_base, array[0].iov_len, 0, array[0].iov_len);
    } else {
        size_t totalbytes = 0;
        for (int i = 0; i < iovcnt; i++) {
            totalbytes += array[i].iov_len;
        }
        wireFormatBuffer = parcBuffer_Allocate(totalbytes);
        for (int i = 0; i < iovcnt; i++) {
            parcBuffer_PutArray(wireFormatBuffer, array[i].iov_len, array[i].iov_base);
        }
        parcBuffer_Flip(wireFormatBuffer);
    }
    return wireFormatBuffer;
}

static int
_internalSEND(_TemplateLinkData *linkData, PARCBuffer *wireFormatBuffer)
{
    size_t length = parcBuffer_Limit(wireFormatBuffer);
    char *buffer = parcBuffer_Overlay(wireFormatBuffer, length);
    // send
}

static PARCBuffer * 
_internalRECEIVE(_TemplateLinkData *linkData);
{
    // Peek at the message header to determine the total length of buffer we need to allocate.
    size_t fixedHeaderLength = ccnxCodecTlvPacket_MinimalHeaderLength();
    PARCBuffer *wireFormatBuffer = parcBuffer_Allocate(fixedHeaderLength);
    const uint8_t *peekBuffer = parcBuffer_Overlay(wireFormatBuffer, 0);
    size_t readCount = 0;

    // readCount = gather fixedHeaderLength bytes into peekBuffer

    // Check for a short header read, since we're only peeking here we can return and retry later
    if (readCount != fixedHeaderLength) {
        linkData->_stats.receive_ReadHeaderFailure++;
        parcBuffer_Release(&wireFormatBuffer);
        return NULL;
    }

    // Obtain the total size of the message from the header
    size_t messageLength = ccnxCodecTlvPacket_GetPacketLength(wireFormatBuffer);

    // Allocate the remainder of message buffer and read a message into it.
    wireFormatBuffer = parcBuffer_Resize(wireFormatBuffer, messageLength);
    char *buffer = parcBuffer_Overlay(wireFormatBuffer, 0);

    // readCount = read remainder of message into buffer

    if (readCount != messageLength) {
        linkData->_stats.receive_ReadError++;
        parcBuffer_Release(&wireFormatBuffer);
        return NULL;
    }

    return wireFormatBuffer;
}

static int
_TemplateSend(AthenaTransportLink *athenaTransportLink, CCNxMetaMessage *ccnxMetaMessage)
{
    struct _TemplateLinkData *linkData = athenaTransportLink_GetPrivateData(athenaTransportLink);

    // Get wire format buffer to send
    PARCBuffer *wireFormatBuffer = ccnxWireFormatMessage_GetWireFormatBuffer(ccnxMetaMessage);

    // If there was no PARCBuffer present, check for an IOvec and convert that into a contiguous buffer.
    if (wireFormatBuffer == NULL) {
        CCNxCodecNetworkBufferIoVec *iovec = ccnxWireFormatMessage_GetIoVec(ccnxMetaMessage);
        assertNotNull(iovec, "Null io vector");
        wireFormatBuffer = _ioVecToBuffer(iovec);
    } else {
        wireFormatBuffer = parcBuffer_Acquire(wireFormatBuffer);
    }

    parcLog_Debug(athenaTransportLink_GetLogger(athenaTransportLink),
                  "sending message (size=%d)", length);

    int result = _internalSEND(linkData, buffer, length);

    parcBuffer_Release(&wireFormatBuffer);
    return 0;
}

static CCNxMetaMessage *
_TemplateReceive(AthenaTransportLink *athenaTransportLink)
{
    struct _TemplateLinkData *linkData = athenaTransportLink_GetPrivateData(athenaTransportLink);
    CCNxMetaMessage *ccnxMetaMessage = NULL;

    PARCBuffer *wireFormatBuffer = _internalRECEIVE(linkData);

    // On error, just return and retry.
    if (wireFormatBuffer == NULL) {
        parcLog_Debug(athenaTransportLink_GetLogger(athenaTransportLink), "read error (%s)", strerror(errno));
        return NULL;
    }

    parcLog_Debug(athenaTransportLink_GetLogger(athenaTransportLink), "received message (size=%d)", readCount);
    parcBuffer_SetPosition(wireFormatBuffer, parcBuffer_Position(wireFormatBuffer) + readCount);
    parcBuffer_Flip(wireFormatBuffer);

    // Construct, and return a ccnxMetaMessage from the wire format buffer.
    ccnxMetaMessage = ccnxMetaMessage_CreateFromWireFormatBuffer(wireFormatBuffer);
    if (ccnxTlvDictionary_GetSchemaVersion(ccnxMetaMessage) == CCNxTlvDictionary_SchemaVersion_V0) {
        parcLog_Warning(athenaTransportLink_GetLogger(athenaTransportLink),
                        "received deprecated version %d message\n", ccnxTlvDictionary_GetSchemaVersion(ccnxMetaMessage));
    }
    if (ccnxMetaMessage == NULL) {
        linkData->_stats.receive_DecodeFailed++;
        parcLog_Error(athenaTransportLink_GetLogger(athenaTransportLink), "Failed to decode message from received packet.");
    }
    parcBuffer_Release(&wireFormatBuffer);

    return ccnxMetaMessage;
}

static void
_TemplateClose(AthenaTransportLink *athenaTransportLink)
{
    parcLog_Info(athenaTransportLink_GetLogger(athenaTransportLink),
                 "link %s closed", athenaTransportLink_GetName(athenaTransportLink));
    _TemplateLinkData *linkData = athenaTransportLink_GetPrivateData(athenaTransportLink);
    _TemplateLinkData_Destroy(&linkData);
}

#include <parc/algol/parc_URIAuthority.h>

#define LINK_NAME_SPECIFIER "name%3D"
#define LOCAL_LINK_FLAG "local%3D"

static AthenaTransportLink *
_TemplateOpen(AthenaTransportLinkModule *athenaTransportLinkModule, PARCURI *connectionURI)
{
    AthenaTransportLink *result = 0;

    // Parse the URI contents to determine the link specific parameters
    const char *authorityString = parcURI_GetAuthority(connectionURI);
    if (authorityString == NULL) {
        parcLog_Error(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                      "Unable to parse connection authority %s", authorityString);
        errno = EINVAL;
        return NULL;
    }
    PARCURIAuthority *authority = parcURIAuthority_Parse(authorityString);
    const char *URIAddress = parcURIAuthority_GetHostName(authority);
    in_port_t port = parcURIAuthority_GetPort(authority);
    parcURIAuthority_Release(&authority);

    char name[MAXPATHLEN] = { 0 };
    char localFlag[MAXPATHLEN] = { 0 };
    int forceLocal = 0;
    char *linkName = NULL;

    PARCURIPath *remainder = parcURI_GetPath(connectionURI);
    size_t segments = parcURIPath_Count(remainder);
    for (int i = 0; i < segments; i++) {
        PARCURISegment *segment = parcURIPath_Get(remainder, i);
        const char *token = parcURISegment_ToString(segment);

        if (strncasecmp(token, LINK_NAME_SPECIFIER, strlen(LINK_NAME_SPECIFIER)) == 0) {
            if (sscanf(token, "%*[^%%]%%3D%s", name) != 1) {
                parcLog_Error(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                              "Improper connection name specification (%s)", token);
                parcMemory_Deallocate(&token);
                errno = EINVAL;
                return NULL;
            }
            linkName = name;
            parcMemory_Deallocate(&token);
            continue;
        }

        if (strncasecmp(token, LOCAL_LINK_FLAG, strlen(LOCAL_LINK_FLAG)) == 0) {
            if (sscanf(token, "%*[^%%]%%3D%s", localFlag) != 1) {
                parcLog_Error(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                              "Improper local specification (%s)", token);
                parcMemory_Deallocate(&token);
                errno = EINVAL;
                return NULL;
            }
            if (strncasecmp(localFlag, "false", strlen("false")) == 0) {
                forceLocal = AthenaTransportLink_ForcedNonLocal;
            } else if (strncasecmp(localFlag, "true", strlen("true")) == 0) {
                forceLocal = AthenaTransportLink_ForcedLocal;
            } else {
                parcLog_Error(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                              "Improper local state specification (%s)", token);
                parcMemory_Deallocate(&token);
                errno = EINVAL;
                return NULL;
            }
            parcMemory_Deallocate(&token);
            continue;
        }

        parcLog_Error(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                      "Unknown connection parameter (%s)", token);
        parcMemory_Deallocate(&token);
        errno = EINVAL;
        return NULL;
    }

    const char *derivedLinkName;

    _TemplateLinkData *linkData = _TemplateLinkData_Create();

    derivedLinkName = _createNameFromLinkData(linkData);

    if (linkName == NULL) {
        linkName = derivedLinkName;
    }

    AthenaTransportLink *athenaTransportLink = athenaTransportLink_Create(linkName,
                                                                          _TemplateSend,
                                                                          _TemplateReceive,
                                                                          _TemplateClose);
    if (athenaTransportLink == NULL) {
        parcLog_Error(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                      "athenaTransportLink_Create failed");
        parcMemory_Deallocate(&derivedLinkName);
        _TemplateLinkData_Destroy(&linkData);
        return athenaTransportLink;
    }

    athenaTransportLink_SetPrivateData(athenaTransportLink, linkData);
    athenaTransportLink_SetLocal(athenaTransportLink, isLocal);

    parcLog_Info(athenaTransportLinkModule_GetLogger(athenaTransportLinkModule),
                 "new link established: Name=\"%s\" (%s)", linkName, derivedLinkName);

    parcMemory_Deallocate(&derivedLinkName);

    // forced IsLocal/IsNotLocal, mainly for testing
    if (result && forceLocal) {
        athenaTransportLink_ForceLocal(result, forceLocal);
    }

    return result;
}

static int
_TemplatePoll(AthenaTransportLink *athenaTransportLink, int timeout)
{
    return 0;
}

PARCArrayList *
athenaTransportLinkModuleTemplate_Init()
{
    // Template module for establishing point to point tunnel connections.
    AthenaTransportLinkModule *athenaTransportLinkModule;
    PARCArrayList *moduleInstanceList = parcArrayList_Create(NULL);
    assertNotNull(moduleInstanceList, "parcArrayList_Create failed to create module list");

    athenaTransportLinkModule = athenaTransportLinkModule_Create("Template",
                                                                 _TemplateOpen,
                                                                 _TemplatePoll);
    assertNotNull(athenaTransportLinkModule, "parcMemory_AllocateAndClear failed allocate Template athenaTransportLinkModule");
    bool result = parcArrayList_Add(moduleInstanceList, athenaTransportLinkModule);
    assertTrue(result == true, "parcArrayList_Add failed");

    return moduleInstanceList;
}
