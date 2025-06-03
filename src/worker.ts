import 'dotenv/config';
const amqp = require('amqplib/callback_api');
import * as fs from 'fs';
import * as path from 'path';
import { exec } from 'child_process';
import * as os from 'os';
import { uploadStream } from './s3upload';

const bucketName = process.env['BUCKET_NAME'];
const rabbitMQHost = process.env['RABBITMQ_URL'] || "amqp://localhost";

if (!bucketName) {
  throw new Error('BUCKET_NAME environment variable is not set');
}

amqp.connect(rabbitMQHost, function(error0:any, connection:any) {
  if (error0) {
    console.log("Can't connect to RabbitMQ. Try checking the RabbitMQ");
    return;
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
    let retryCount = 0;
    const MAX_RETRIES = 2;

    channel.consume(queue,function(msg:any) {
      console.log("Received the message from queue");
      const message = JSON.parse(msg.content.toString());
      const script = message.script;
      const scriptId = message.scriptId;
      const sceneName = message.sceneName;
      const correlationId=message.correlationId;

      var responseQueue = `response_queue_${correlationId}`;
      channel.assertQueue(responseQueue, {
        durable: true,
        autoDelete: true,
      });
      
      // Create temporary Python file
      const tempDir = os.tmpdir();
      const tempPythonFile = path.join(tempDir, `script_${scriptId}.py`);
      
      try {
        fs.writeFileSync(tempPythonFile, script);
        
        // Execute the Manim script as a node js child process
        exec(`python -m manim ${tempPythonFile} ${sceneName}`, async (error, stdout, stderr) => {
          if (error) {
            console.error(`Error executing script: ${error}`);
            retryCount++;
            
            if (retryCount >= MAX_RETRIES) {
              console.log(`Max retries (${MAX_RETRIES}) reached, acknowledging message`);
              channel.ack(msg);
              const responseMessage = {
                scriptId,
                isUploaded:false
              };
              const responseData = JSON.stringify(responseMessage);
    
              channel.sendToQueue(responseQueue, Buffer.from(responseData), {
                  persistent: true,
                  correlationId,
              });
              const cleanupDirPath = path.join(process.cwd(), 'media');
              if (fs.existsSync(cleanupDirPath)) {
                fs.rmSync(cleanupDirPath, { recursive: true, force: true });
                console.log(`Cleaned up: ${cleanupDirPath}`);
              }
              retryCount = 0;
            } else {
              console.log(`Retry attempt ${retryCount} of ${MAX_RETRIES}`);
              channel.nack(msg, false, true); // Requeue the message
            }
            return;
          }
          
          // Reset retry count on successful execution
          retryCount = 0;
          
          // Find the generated video file in the media directory
          const mediaDir = path.join(process.cwd(), 'media', 'videos', `script_${scriptId}`,'1080p60');
          const videoFiles = fs.readdirSync(mediaDir);
          const videoFile = videoFiles.find(file => file.endsWith('.mp4'));
          
          if (videoFile) {
            // Rename the video file
            const newVideoName = `output_${scriptId}.mp4`;
            const sourcePath = path.join(mediaDir, videoFile);
            const targetPath = path.join(mediaDir, newVideoName);
            
            // Rename the file
            fs.renameSync(sourcePath, targetPath);

            try {
              // Upload to S3
              const { writeStream, promise } = uploadStream(bucketName, newVideoName, targetPath);
              fs.createReadStream(targetPath).pipe(writeStream);

              const uploadResult = await promise;
              console.log(uploadResult);
              console.log("Video uploaded to s3");

              if(uploadResult){
                // Send the VideoURL to Rabbit MQ
                  try{
                    const responseMessage = {
                      scriptId,
                      isUploaded:true
                    };
                    const responseData = JSON.stringify(responseMessage);
    
                    channel.sendToQueue(responseQueue, Buffer.from(responseData), {
                        persistent: true,
                        correlationId
                    });
                    console.log(" [x] Response Sent");
                  }
                  catch(error){
                    throw new Error(`Error while sending response to queue: ${error}`);
                  }
              }
            }
            catch (error) {
              console.error("Error in upload process:", error);
            } finally {
              // Cleanup
              const cleanupDirPath = path.join(process.cwd(), 'media');
              if (fs.existsSync(cleanupDirPath)) {
                fs.rmSync(cleanupDirPath, { recursive: true, force: true });
                console.log(`Cleaned up: ${cleanupDirPath}`);
              }
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