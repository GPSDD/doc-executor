const logger = require('logger');
const csv = require('fast-csv');
const fs = require('fs');
const UrlNotFound = require('errors/urlNotFound');
const randomstring = require('randomstring');

const DownloadService = require('services/downloadService');
const FileNotFound = require('errors/fileNotFound');

//the hdx files have an extra header row, client have wanted to keep the second hash row - we strip the first row here.

class CSVConverter {

    constructor(url, verify = false, delimiter = ',') {
        this.checkURL = new RegExp('^(https?:\\/\\/)?' + // protocol
            '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.?)+[a-z]{2,}|' + // domain name
            '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
            '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
            '(\\?[:;&a-z\\d%_.~+=-]*)?' + // query string
            '(\\#[-a-z\\d_]*)?$', 'i');
        this.delimiter = delimiter;
        this.url = url;
        this.verify = verify;
    }

    async init() {
        if (this.checkURL.test(this.url)) {
            logger.debug('Is a url. Downloading file');
            const exists = await DownloadService.checkIfExists(this.url);
            if (!exists) {
                throw new UrlNotFound(400, 'Url not found');
            }
            let name = randomstring.generate();
            if (this.delimiter === '\t') {
                name += '.tsv';
            } else {
                name += '.csv';
            }
            const result = await DownloadService.downloadFile(this.url, name, this.verify);
            this.filePath = result.path;

            this.sha256 = result.sha256;
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        }
        
        this.checkAndFormatHXL();
        
        logger.debug("Second filepath")
        logger.debug(this.filePath)
        const readStream = csv.fromPath(this.filePath, {
            headers: true,
            delimiter: this.delimiter,
            discardUnmappedColumns: true
        });
        readStream.on('end', () => {
            logger.info('Removing file', this.filePath);
            if (fs.existsSync(this.filePath) && !this.verify) {
                fs.unlinkSync(this.filePath);
            }
        });

        return readStream;
    }

    checkAndFormatHXL() {
        logger.debug("check and format")
        let isHXL = false;
        let rowCount = 0;
        //first check if isHXL
        const readStream = csv.fromPath(this.filePath, {
            headers: false,
            delimiter: ',',
            discardUnmappedColumns: true
        });
        readStream.on('data', (row) => {
            if(rowCount === 1) {
                logger.debug(row)
                if(row[0].indexOf('#') > -1) {
                    isHXL = true;            
                }
            }
            rowCount++;
        })
        if(!isHXL) {
            return;
        } 
        logger.debug("ISHXL")
        //if HXL - strip first row
        let name = randomstring.generate();
        const path = `/tmp/${name}`;
        rowCount = 0;
        const transformStream = csv.fromPath(filePath, {
            headers: false,
            delimiter: ',',
            discardUnmappedColumns: true
        })
        transformStream.transform((row) => {
            if(rowCount === 0) {
                rowCount++;
                return;
            }
            rowCount++;
            return row;
        })
        .pipe(csv.createWriteStream({headers: true}))
        .pipe(fs.createWriteStream(path, {encoding: "utf8"}));

        if (fs.existsSync(this.filePath)) {
            fs.unlinkSync(this.filePath);
        }

        logger.debug(this.filePath)
        this.filePath = path;
    }    
}

module.exports = CSVConverter;
