'use strict';

const requestPromise = require('request-promise');
const fs = require('fs');
const logger = require('logger');
const Bluebird = require('bluebird');
const https = require('https');
const http = require('http');
const crypto = require('crypto');
const algorithm = 'sha256';

const download = require('download');

function humanFileSize(bytes, si) {
    const thresh = si ? 1000 : 1024;
    if (Math.abs(bytes) < thresh) {
        return `${bytes} B`;
    }
    const units = si ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while (Math.abs(bytes) >= thresh && u < units.length - 1);
    return `${bytes.toFixed(1)} ${units[u]}`;
}

const requestDownloadFile = function (url, path, verify) {

    return new Bluebird(function (resolve, reject) {
        logger.debug('Sending request');
        try {
            download(url).then(data => {
                fs.writeFileSync(path, data);
                logger.info(`File size: ${humanFileSize(parseInt(data.length, 10))}`);
                resolve();
            }).catch((err) => {
                logger.error('Error downloading file', err);
                reject(err);
            });
          
        } catch (err) {
            logger.error(err);
            reject(err);
        }
    });

};

class DownloadService {

    static async checkIfExists(url) {
        logger.info(`Checking if the url ${url} exists`);
        try {
            return true; //doing this for now as i'm getting false 404's for many hdx urls
            // const result = await requestPromise.head({
            //     url,
            //     simple: false,
            //     resolveWithFullResponse: true
            // });
            // logger.debug('Headers ', result.headers['content-type'], result.statusCode);

            // return result.statusCode === 200;
        } catch(err) {
            logger.error(err);
            return false;
        }
    }

    static async downloadFile(url, name, verify) {
        logger.debug('Downloading....');
        const path = `/tmp/${name}`;
        logger.debug('Temporal path', path, '. Downloading');
        const sha256 = await requestDownloadFile(url, path, verify);
        logger.debug('Downloaded file!!!');
        return {
            path,
            sha256
        };
    }


}

module.exports = DownloadService;
