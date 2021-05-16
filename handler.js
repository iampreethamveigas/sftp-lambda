var AWS = require('aws-sdk');
var s3 = new AWS.S3();
let Client = require('ssh2-sftp-client');
let sftp = new Client();
const stream = require('stream');
var glue = new AWS.Glue();
const SFTP_CONFIG = require('./config/_config.sftp')
const DATABASE_CONFIG = require('./config/_config.db')




let SFTP_FOLDER = '/Inbox';
const folder = '/tmp/temp.txt';
const crawlerName1 = 'ive-dev-datamart-fixedlength-file-crawler1';
const crawlerName2 = 'ive-dev-datamart-fixedlength-file-crawler2';


/* 
S3 file upload
*/
const uploadStream = ({ Bucket, Key }) => {
    const pass = new stream.PassThrough();
    return {
        writeStream: pass,
        promise: s3.upload({ Bucket, Key, Body: pass }).promise(),
    };
}




/** Start the ETL glue job to insert data to vehicle_data table 
 * Source data catalog table name is sent as a parameter to the glue job */

function callGlueJob(filename) {
    return new Promise(async (resolve, reject) => {
        try {

            console.log("call glue job" + filename);
            var params = {
                JobName: 'ive_dev_partssummary',
                Arguments: {
                    '--sourceFile': filename,
                    '--glueRegion': 'us-west-2',
                    '--crawlerName1': crawlerName1,
                    '--crawlerName2': crawlerName2,
                    '--glueDb': 'inputdb',
                }
            };


            //Invoke job run
            glue.startJobRun(params, async (err, data) => {
                if (err) reject(err)
                else
                    // successful response
                    let jobId = data['JobRunId'];
                let params = {
                    /* required */
                    JobName: 'ive_dev_partssummary',
                    /* required */
                    RunId: jobId,
                };
                let status = await glue.getJobRun(params, function (err, data) {
                    if (err) console.log(err, err.stack); // an error occurred
                    else console.log(data); // successful response
                }).promise();
                console.log('status ' + status);
                resolve(status);

            });

        }
        catch (err) {
            console.log(err);
            reject(err);
        }
    })


}

/* 
fn to remove the extension of a file
*/
const removeExtension = (filename) => {
    // get the last dot
    const fileName_length_Without_Extension = filename.split('.').length - 1;
    const filename_in_array = filename.split('.');
    let name = ''
    for (let i = 0; i < fileName_length_Without_Extension; i++) {
        name += filename_in_array[i]
    }
    return name
}


/* Fn to split the file name */
const splitfileNameAndExtension = (filename) => {
    // get the last dot
    let extension = filename.substring(filename.lastIndexOf('.') + 1, filename.length) != '.txt' ? '' : filename.substring(filename.lastIndexOf('.') + 1, filename.length)
    let name = extension.length > 0 ? filename.replace('.' + extension, '') : filename
    return {
        name,
        extension
    }
}




const process_file = (event) => new Promise(async (res, rej) => {
    await sftp.connect(SFTP_CONFIG)
        .then(async (chunk) => new Promise(async (res, rej) => {
            // let fileWtr = fs.createWriteStream('/tmp/sample.txt.gz');
            let fileWtr = fs.createWriteStream(folder);
            fileWtr.on('open', function (fd) {
                console.log((fd))
            })
            readStream.on('open', function () {
                // This just pipes the read stream to the response object (which goes to the client)
                const { writeStream, promise } = uploadStream({ Bucket: 'ive-dev-parts-info', Key: `uploadingSample` });
                readStream.pipe(writeStream);
                promise.then(e => {
                    res(e)
                }).catch(e => rej(e))
            });

            readStream.on('error', function (err) {
                rej(err);
            });
            let out = fs.createWriteStream(folder, {
                flags: 'w',
                encoding: null
            });
            await sftp.get('/Inbox/TestResponse/GBE3BKZ1.DBK5Z01D.X0000001', out).catch(e => console.log("catch", e))
            sftp.end()
        }))
        .catch(e => rej('ddada'))
})

/**
 * Fn is RND
 * agenda is to pull the file from onprem sftp push to s3 using lambda and cloudwatch event
 * 
 */
exports.handler = async function (event, context, callback) {
    try {

        console.time('---Job Processig Inintiated---')
        let res_go = await process_file(event);
        /*
            ## This function wil take 
            ## glue params 
            ## s3 params
            ## sftp params
            ## params for deletion operation flag on s3 and sftp optionaly 
            
        */
        const filename = event['filename'] || 'Parts_Info_TMMK.txt'
        const S3targets3bucket = event['target_s3_bucket'] || 'ive-dev-parts-info'
        const sftpsourceFolder = event['sftp_source_Folder'] || SFTP_FOLDER
        let shouldDeletefromdestination = event['shouldDeletefromdestination'] || true
        let TriggerGlue = event['TriggerGlue'] || true
        let glueParams = event['glueParams'] || false
        // ## could be altered based on the params or arugs from input
        const S3DestFilename = S3targets3bucket

        const fileNameAndExtension = await splitfileNameAndExtension(filename)
        console.log(fileNameAndExtension, 'fileNameAndExtension')

        console.log('deleting the file')
        const deleteFileFromSftp = () => new Promise(async (res, rej) => {
            await sftp.connect(SFTP_CONFIG)
                .then(() => {
                    sftp.delete(`${sftpsourceFolder}/${filename}`).then(() => {
                        console.log("File deleted from sftp " + `${SFTP_FOLDER}/${filename}`)
                        sftp.end();
                    }).catch((err) => {
                        rej(err)
                        console.log("error while deleting file from sftp " + `${SFTP_FOLDER}/${filename}`)
                        sftp.end();

                    });

                })
                .catch(() => { })
        })




        const write = file => {
            return new Promise((res, rej) => {
                fs.writeFile(file, JSON.stringify({ message: '' }), async (err) => {
                    if (err) {
                        return rej(err)
                    }

                    let option = {
                        concurrency: 640, // integer. Number of concurrent reads to use
                        chunkSize: 32768, // integer. Size of each read in bytes
                        // step: function(total_transferred, chunk, total) {
                        //     // console.log(total_trans+ferred, chunk, total)
                        // }
                    }
                    console.log('------prior to sftp connection----')
                    console.log(`${sftpsourceFolder}/${filename}`)

                    await sftp.connect(SFTP_CONFIG)
                        .then(() => sftp.get(`${sftpsourceFolder}/${filename}`))
                        .then(async (chunk) => {
                            sftp.end()
                            console.log('here file will be moved')


                            let promise = s3.upload({ Bucket: S3targets3bucket, Key: filename, Body: chunk }).promise();
                            console.log('here file is moved')
                            console.log('------then block sftp connection----')
                            // const { writeStream, promise } = uploadStream({ Bucket: S3targets3bucket, Key: `${filename}` });
                            // writeStream.end(chunk)
                            // const readStream = writeStream.end(chunk)
                            // readStream.pipe(writeStream);
                            promise
                                .then((e) => {
                                    res(e)
                                    console.log('------error in moving file----')
                                    console.log('------Promise block sftp connection----')
                                })
                                .catch(er => rej(er))

                        })
                        .catch(err => {
                            console.log('error--')
                            console.log('------then block sftp connection----')
                            console.log(err)
                        })
                })
            })
        }


        return write(folder).then(async res => {
            console.log('------return block after write----')
            console.log(shouldDeletefromdestination + 'shouldDeletefromdestination')
            //  if (shouldDeletefromdestination) {
            //        deleteFileFromSftp()
            //   }
            console.log(TriggerGlue + 'TriggerGlue')
            const filenameforglue = fileNameAndExtension.name + '_' + fileNameAndExtension.extension
             const statusofS3 = TriggerGlue ? await callGlueJob(filenameforglue) : null
            return statusofS3
        }).catch(err => {
            console.log(('error while s3 write process flow'))
            throw err
        })


    }
    catch (e) {
        throw e
    }
}
