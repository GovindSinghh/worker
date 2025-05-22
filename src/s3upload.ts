import AWS from 'aws-sdk';
import stream from 'stream';
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
const s3=new AWS.S3();// to upload the data
const s3Client=new S3Client({// to get presigned url
    region:'ap-south-1b'
})

export const uploadStream = (bucketName:string, s3Key:string) => {
    const pass = new stream.PassThrough();
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

export async function generatePresignedUrl(
    bucketName: string,
    objectKey: string,
    ): Promise<string> {

        const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: objectKey,// filename
        });

    const signedUrl = await getSignedUrl(s3Client, command);
    return signedUrl;
}