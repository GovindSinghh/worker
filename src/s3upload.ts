import { PassThrough } from 'stream';
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
const s3Client = new S3Client({
    region: 'ap-south-1'
});

export const uploadStream = (bucketName:string, s3Key:string) => {
    const pass = new PassThrough();
    const command = new PutObjectCommand({
        Bucket: bucketName,
        Key: s3Key,
        Body: pass,
        ContentType: 'video/mp4'
    });
    return {
        writeStream: pass,
        promise: s3Client.send(command)
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