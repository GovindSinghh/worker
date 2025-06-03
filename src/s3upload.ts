import { PassThrough } from 'stream';
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import * as fs from 'fs';

const s3Client = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
    }
});

export const uploadStream = (bucketName:string, s3Key:string, filePath: string) => {
    const pass = new PassThrough();
    const fileSize = fs.statSync(filePath).size;
    
    const command = new PutObjectCommand({
        Bucket: bucketName,
        Key: s3Key,
        Body: pass,
        ContentType: 'video/mp4',
        ContentLength: fileSize
    });
    return {
        writeStream: pass,
        promise: s3Client.send(command)
    };
};