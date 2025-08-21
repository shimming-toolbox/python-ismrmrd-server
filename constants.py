import struct
import uuid

MRD_MESSAGE_INT_ID_MIN                             =    0 # CONTROL
MRD_MESSAGE_CONFIG_FILE                            =    1
MRD_MESSAGE_CONFIG_TEXT                            =    2
MRD_MESSAGE_METADATA_XML_TEXT                      =    3
MRD_MESSAGE_CLOSE                                  =    4
MRD_MESSAGE_TEXT                                   =    5
MRD_MESSAGE_INT_ID_MAX                             =  999 # CONTROL
MRD_MESSAGE_EXT_ID_MIN                             = 1000 # CONTROL
MRD_MESSAGE_ACQUISITION                            = 1001 # DEPRECATED
MRD_MESSAGE_NEW_MEASUREMENT                        = 1002 # DEPRECATED
MRD_MESSAGE_END_OF_SCAN                            = 1003 # DEPRECATED
MRD_MESSAGE_IMAGE_CPLX_FLOAT                       = 1004 # DEPRECATED
MRD_MESSAGE_IMAGE_REAL_FLOAT                       = 1005 # DEPRECATED
MRD_MESSAGE_IMAGE_REAL_USHORT                      = 1006 # DEPRECATED
MRD_MESSAGE_EMPTY                                  = 1007 # DEPRECATED
MRD_MESSAGE_ISMRMRD_ACQUISITION                    = 1008
MRD_MESSAGE_ISMRMRD_IMAGE_CPLX_FLOAT               = 1009 # DEPRECATED
MRD_MESSAGE_ISMRMRD_IMAGE_REAL_FLOAT               = 1010 # DEPRECATED
MRD_MESSAGE_ISMRMRD_IMAGE_REAL_USHORT              = 1011 # DEPRECATED
MRD_MESSAGE_DICOM                                  = 1012 # DEPRECATED
MRD_MESSAGE_CLOUD_JOB                              = 1013 # UNSUPPORTED
MRD_MESSAGE_GADGETCLOUD_JOB                        = 1014 # UNSUPPORTED
MRD_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_CPLX_FLOAT     = 1015 # DEPRECATED
MRD_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_FLOAT     = 1016 # DEPRECATED
MRD_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_USHORT    = 1017 # DEPRECATED
MRD_MESSAGE_DICOM_WITHNAME                         = 1018 # UNSUPPORTED
MRD_MESSAGE_DEPENDENCY_QUERY                       = 1019 # UNSUPPORTED
MRD_MESSAGE_ISMRMRD_IMAGE_REAL_SHORT               = 1020 # DEPRECATED
MRD_MESSAGE_ISMRMRD_IMAGEWITHATTRIB_REAL_SHORT     = 1021 # DEPRECATED
MRD_MESSAGE_ISMRMRD_IMAGE                          = 1022
MRD_MESSAGE_RECONDATA                              = 1023 # UNSUPPORTED
MRD_MESSAGE_ISMRMRD_WAVEFORM                       = 1026
MRD_MESSAGE_ISMRMRD_FEEDBACK                       = 1028
MRD_MESSAGE_ISMRMRD_STORE_DATA_QUERY               = 1030
MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE            = 1031
MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY            = 1032
MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_RESPONSE         = 1033
MRD_MESSAGE_EXT_ID_MAX                             = 4096 # CONTROL

MrdMessageLength = struct.Struct('<I') # little-endian uint32 (4 bytes)
SIZEOF_MRD_MESSAGE_LENGTH = len(MrdMessageLength.pack(0))

MrdMessageIdentifier = struct.Struct('<H') # little-endian short (2 bytes)
SIZEOF_MRD_MESSAGE_IDENTIFIER = len(MrdMessageIdentifier.pack(0))

MrdMessageConfigurationFile = struct.Struct('<1024s') # little-endian 1024 char (1024 bytes)
SIZEOF_MRD_MESSAGE_CONFIGURATION_FILE = len(MrdMessageConfigurationFile.pack(b''))

MrdMessageAttribLength = struct.Struct('<Q') # little-endian unsigned long long (8 bytes)
SIZEOF_MRD_MESSAGE_ATTRIB_LENGTH = len(MrdMessageAttribLength.pack(0))

MrdMessageStatus = struct.Struct('<I') # little-endian uint32 (4 bytes)
SIZEOF_MRD_MESSAGE_STATUS = len(MrdMessageStatus.pack(0))

SIZEOF_MRD_MESSAGE_UUID = len(uuid.uuid4().bytes)

# Logging serverity levels
MRD_LOGGING_DEBUG    = "DEBUG   "
MRD_LOGGING_INFO     = "INFO    "
MRD_LOGGING_WARNING  = "WARNING "
MRD_LOGGING_ERROR    = "ERROR   "
MRD_LOGGING_CRITICAL = "CRITICAL"

# Structs for data storage
class MrdStoreDataQuery:
    uid = None
    name = ""
    data = b''

    def __init__(self, name, data, uid=uuid.uuid4()):
        self.uid  = uid
        self.name = name
        self.data = data

class MrdStoreDataResponse:
    uid = None
    status = 0

    def __init__(self, status, uid=uuid.uuid4()):
        self.uid  = uid
        self.status = status

class MrdRetrieveDataQuery:
    uid = None
    name = ""

    def __init__(self, name, uid=uuid.uuid4()):
        self.uid  = uid
        self.name = name

class MrdRetrieveDataResponse:
    uid = None
    status = 0
    name = ""
    data = b''

    def __init__(self, status, name, data, uid=uuid.uuid4()):
        self.uid    = uid
        self.status = status
        self.name   = name
        self.data   = data