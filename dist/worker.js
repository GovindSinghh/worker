"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const amqp = require('amqplib/callback_api');
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const child_process_1 = require("child_process");
const os = __importStar(require("os"));
const s3upload_1 = require("./s3upload");
const pg_1 = require("pg");
const bucketName = process.env['BUCKET_NAME'];
const rabbitMQHost = process.env['RABBITMQ_HOST'] || 'localhost';
// Database connection
const pool = new pg_1.Pool({
    connectionString: process.env['DATABASE_URL'],
    ssl: {
        rejectUnauthorized: false
    }
});
if (!bucketName) {
    throw new Error('BUCKET_NAME environment variable is not set');
}
function storeVideoUrl(scriptId, videoUrl) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const query = 'UPDATE script SET videoUrl = $1 WHERE id = $2';
            yield pool.query(query, [videoUrl, scriptId]);
            console.log('Video URL stored in database successfully');
        }
        catch (error) {
            console.error('Error storing video URL:', error);
            throw error;
        }
    });
}
amqp.connect(`amqp://${rabbitMQHost}`, function (error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function (error1, channel) {
        if (error1) {
            throw error1;
        }
        var queue = 'task_queue';
        channel.assertQueue(queue, {
            durable: true
        });
        // channel.prefetch(1); use when you are running multiple workers so that ( 1 worker- 1 task ) at a time
        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
        channel.consume(queue, function (msg) {
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
                (0, child_process_1.exec)(`python -m manim ${tempPythonFile} ${sceneName}`, (error, stdout, stderr) => __awaiter(this, void 0, void 0, function* () {
                    if (error) {
                        console.error(`Error executing script: ${error}`);
                        return;
                    }
                    // Find the generated video file in the media directory
                    const mediaDir = path.join(process.cwd(), 'media', 'videos', `script_${scriptId}`, '1080p60');
                    const videoFiles = fs.readdirSync(mediaDir);
                    const videoFile = videoFiles.find(file => file.endsWith('.mp4'));
                    if (videoFile) {
                        // Rename the video file
                        const newVideoName = `output_${scriptId}.mp4`;
                        const sourcePath = path.join(mediaDir, videoFile);
                        // Rename the file
                        fs.renameSync(sourcePath, path.join(mediaDir, newVideoName));
                        // write the code to store the video to s3
                        const { writeStream, promise } = yield (0, s3upload_1.uploadStream)(bucketName, newVideoName);
                        fs.createReadStream(path.join(mediaDir, newVideoName)).pipe(writeStream);
                        promise.then((data) => __awaiter(this, void 0, void 0, function* () {
                            console.log("Video uploaded to s3 at location :", data.Location);
                            const presignedUrl = yield (0, s3upload_1.generatePresignedUrl)(bucketName, newVideoName);
                            console.log("Presigned url :", presignedUrl);
                            // Store URL in database
                            yield storeVideoUrl(scriptId, presignedUrl);
                        })).catch((error) => {
                            console.log("Error uploading the file to s3 : ", error);
                        });
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
                }));
            }
            catch (error) {
                console.error(`Error processing script: ${error}`);
                channel.ack(msg);
            }
        }, {
            noAck: false
        });
    });
});
