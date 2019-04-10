package com.ytfs.service.packet;

public class ServiceErrorCode {

    public static final int SERVER_ERROR = 0x00;//服务区内部错误
    public static final int INVALID_USER_ID = 0x01;//用户ID不存在
    public static final int NOT_ENOUGH_DHH = 0x02;//HDD空间不足
    public static final int INTERNAL_ERROR = 0x03;//网络故障
    public static final int INVALID_UPLOAD_ID = 0x04;//对话失效
    public static final int TOO_MANY_SHARDS = 0x05;//分片太多
    public static final int ILLEGAL_VHP_NODEID = 0x06;//数据块所属节点不一致
    public static final int NO_SUCH_BLOCK = 0x07;//找不到数据块元数据
    public static final int INVALID_VHB = 0x08;
    public static final int INVALID_VHP = 0x09;
    public static final int INVALID_KED = 0x0a;
    public static final int INVALID_KEU = 0x0b;
    public static final int INVALID_VHW = 0x0c;
    public static final int TOO_BIG_BLOCK = 0x0d;
    public static final int INVALID_SIGNATURE = 0x0e;
    public static final int INVALID_NODE_ID = 0x0f;
    public static final int INVALID_SHARD = 0x10;
    
    
    /*
    public static final String ACCESS_DENIED = "AccessDenied";
    public static final String ACCESS_FORBIDDEN = "AccessForbidden";
    public static final String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
    public static final String BUCKET_NOT_EMPTY = "BucketNotEmpty";
    public static final String FILE_GROUP_TOO_LARGE = "FileGroupTooLarge";
    public static final String FILE_PART_STALE = "FilePartStale";
    public static final String INVALID_ARGUMENT = "InvalidArgument";
    public static final String INVALID_BUCKET_NAME = "InvalidBucketName";
    public static final String INVALID_OBJECT_NAME = "InvalidObjectName";
    public static final String INVALID_PART = "InvalidPart";
    public static final String INVALID_PART_ORDER = "InvalidPartOrder";
    public static final String INVALID_TARGET_BUCKET_FOR_LOGGING = "InvalidTargetBucketForLogging";

    public static final String MISSING_CONTENT_LENGTH = "MissingContentLength";
    public static final String MISSING_ARGUMENT = "MissingArgument";
    public static final String NO_SUCH_BUCKET = "NoSuchBucket";
    public static final String NO_SUCH_KEY = "NoSuchKey";
    public static final String NOT_IMPLEMENTED = "NotImplemented";
    public static final String PRECONDITION_FAILED = "PreconditionFailed";
    public static final String NOT_MODIFIED = "NotModified";
    public static final String INVALID_LOCATION_CONSTRAINT = "InvalidLocationConstraint";
    public static final String ILLEGAL_LOCATION_CONSTRAINT_EXCEPTION = "IllegalLocationConstraintException";
    public static final String REQUEST_TIME_TOO_SKEWED = "RequestTimeTooSkewed";
    public static final String REQUEST_TIMEOUT = "RequestTimeout";
    public static final String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";
    public static final String TOO_MANY_BUCKETS = "TooManyBuckets";
    public static final String NO_SUCH_CORS_CONFIGURATION = "NoSuchCORSConfiguration";
    public static final String NO_SUCH_WEBSITE_CONFIGURATION = "NoSuchWebsiteConfiguration";
    public static final String NO_SUCH_LIFECYCLE = "NoSuchLifecycle";
    public static final String MALFORMED_XML = "MalformedXML";
    public static final String INVALID_ENCRYPTION_ALGORITHM_ERROR = "InvalidEncryptionAlgorithmError";
    public static final String NO_SUCH_UPLOAD = "NoSuchUpload";
    public static final String ENTITY_TOO_SMALL = "EntityTooSmall";
    public static final String ENTITY_TOO_LARGE = "EntityTooLarge";
    public static final String INVALID_DIGEST = "InvalidDigest";
    public static final String INVALID_RANGE = "InvalidRange";
    public static final String SECURITY_TOKEN_NOT_SUPPORTED = "SecurityTokenNotSupported";
    public static final String OBJECT_NOT_APPENDALBE = "ObjectNotAppendable";
    public static final String POSITION_NOT_EQUAL_TO_LENGTH = "PositionNotEqualToLength";
    public static final String INVALID_RESPONSE = "InvalidResponse";
    public static final String CALLBACK_FAILED = "CallbackFailed";
    public static final String NO_SUCH_LIVE_CHANNEL = "NoSuchLiveChannel";
    public static final String NO_SUCH_SYM_LINK_TARGET = "SymlinkTargetNotExist";
    public static final String INVALID_OBJECT_STATE = "InvalidObjectState";
     */
}
