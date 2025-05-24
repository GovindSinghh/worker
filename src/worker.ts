import 'dotenv/config';
const amqp = require('amqplib/callback_api');
import * as fs from 'fs';
import * as path from 'path';
import { exec } from 'child_process';
import * as os from 'os';
import { generatePresignedUrl, uploadStream } from './s3upload';
import { Pool } from "pg";

const bucketName = process.env['BUCKET_NAME'];
const rabbitMQHost = process.env['RABBITMQ_HOST'] || 'localhost';

// Database connection
const pool = new Pool({
  connectionString: process.env['DATABASE_URL'],
  ssl: {
    rejectUnauthorized: false
  }
});

if (!bucketName) {
  throw new Error('BUCKET_NAME environment variable is not set');
}

async function storeVideoUrl(scriptId: string, videoUrl: string) {
  try {
    const query = 'UPDATE script SET videoUrl = $1 WHERE id = $2';
    await pool.query(query, [videoUrl, scriptId]);
    console.log('Video URL stored in database successfully');
  } catch (error) {
    console.error('Error storing video URL:', error);
    throw error;
  }
}
// Add rabbitMQ connection after deploying
amqp.connect(`amqp://${rabbitMQHost}`, function(error0:any, connection:any) {
  if (error0) {
    throw error0;
  }
  connection.createChannel(function(error1:any, channel:any) {
    if (error1) {
      throw error1;
    }
    var queue = 'task_queue';

    channel.assertQueue(queue, {
      durable: true
    });
    // channel.prefetch(1); use when you are running multiple workers so that ( 1 worker- 1 task ) at a time
    console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
    channel.consume(queue,function(msg:any) {

      console.log("Received the message from queue");

      const message = JSON.parse(msg.content.toString());
      const script = message.script;
      const scriptId = message.scriptId;
      const sceneName = message.sceneName;
      
      // Create temporary Python file
      const tempDir = os.tmpdir();
      const tempPythonFile = path.join(tempDir, `script_${scriptId}.py`);
      
      try {
        fs.writeFileSync(tempPythonFile, script);
        
        // Execute the Manim script as a node js child process
        exec(`python -m manim ${tempPythonFile} ${sceneName}`, async (error, stdout, stderr) => {
          if (error) {
            console.error(`Error executing script: ${error}`);
            return;
          }
          
          // Find the generated video file in the media directory
          const mediaDir = path.join(process.cwd(), 'media', 'videos', `script_${scriptId}`,'1080p60');
          const videoFiles = fs.readdirSync(mediaDir);
          const videoFile = videoFiles.find(file => file.endsWith('.mp4'));
          
          if (videoFile) {
            // Rename the video file
            const newVideoName = `output_${scriptId}.mp4`;
            const sourcePath = path.join(mediaDir, videoFile);
            
            // Rename the file
            fs.renameSync(sourcePath, path.join(mediaDir, newVideoName));

            // write the code to store the video to s3
            const { writeStream, promise }=await uploadStream(bucketName,newVideoName);
            fs.createReadStream(path.join(mediaDir,newVideoName)).pipe(writeStream);

            promise.then(async (data)=>{
              console.log("Video uploaded to s3")
            }).catch((error)=>{
              console.log("Error uploading the video file to s3 : ",error);
            });

            const presignedUrl=await generatePresignedUrl(bucketName,newVideoName);
            console.log("Presigned url :",presignedUrl);
            // Store URL in database
            await storeVideoUrl(scriptId, presignedUrl);
            
            const cleanupDirPath = path.join(process.cwd(), 'media');
            if (fs.existsSync(cleanupDirPath)) {
              fs.rmSync(cleanupDirPath, { recursive: true, force: true });
              console.log(`Cleaned up: ${cleanupDirPath}`);
            }
          }
          
          // Clean up temporary Python file
          fs.unlinkSync(tempPythonFile);
          
          // acknowledge so RabbitMQ deletes the msg from queue
          channel.ack(msg);
        });
      } catch (error) {
        console.error(`Error processing script or uploading video: ${error}`);
        channel.ack(msg);
      }
    }, {
      noAck: false
    });
  });
});