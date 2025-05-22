"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.uploadStream = void 0;
exports.generatePresignedUrl = generatePresignedUrl;
const aws_sdk_1 = __importDefault(require("aws-sdk"));
const stream_1 = require("stream");
const client_s3_1 = require("@aws-sdk/client-s3");
const s3_request_presigner_1 = require("@aws-sdk/s3-request-presigner");
const s3 = new aws_sdk_1.default.S3(); // to upload the data
const s3Client = new client_s3_1.S3Client({
    region: 'ap-south-1b'
});
const uploadStream = (bucketName, s3Key) => {
    const pass = new stream_1.PassThrough();
    const params = {
        Bucket: bucketName,
        Key: s3Key,
        Body: pass,
        ContentType: 'video/mp4'
    };
    return {
        writeStream: pass,
        promise: s3.upload(params).promise()
    };
};
exports.uploadStream = uploadStream;
function generatePresignedUrl(bucketName, objectKey) {
    return __awaiter(this, void 0, void 0, function* () {
        const command = new client_s3_1.GetObjectCommand({
            Bucket: bucketName,
            Key: objectKey, // filename
        });
        const signedUrl = yield (0, s3_request_presigner_1.getSignedUrl)(s3Client, command);
        return signedUrl;
    });
}
