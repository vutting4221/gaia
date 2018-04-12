import azure from 'azure-storage'
import logger from 'winston'
import { BadPathError, StorageLimitError } from '../errors'

// The AzDriver utilized the azure nodejs sdk to write files to azure blob storage
class AzDriver {

  constructor (config) {
    this.blobService = azure.createBlobService(config.azCredentials.accountName,config.azCredentials.accountKey)
    this.bucket = config.bucket
    this.accountName = config.azCredentials.accountName
    this.readURL = config.readURL
    this.cacheControl = config.cacheControl
    if (config.storageLimit) {
      this.storageLimit = config.storageLimit
    } else {
      this.storageLimit = 0
    }

    // Check for container(bucket), create it if does not exist
    // Set permissions to 'blob' to allow public reads
    this.blobService.createContainerIfNotExists(
      config.bucket, { publicAccessLevel: 'blob' },
      (error) => {
        if (error) {
          logger.error(`failed to initialize azure container: ${error}`)
          throw error
        }
        logger.info('container initialized.')
      })
  }

  static isPathValid (path) {
    // for now, only disallow double dots.
    return (path.indexOf('..') === -1)
  }

  getReadURLPrefix () {
    if (this.readURL) {
      return `https://${this.readURL}/${this.bucket}/`
    }
    return `https://${this.accountName}.blob.core.windows.net/${this.bucket}/`
  }

  innerListBlob (prior) {
    if (prior.attempts >= 5) {
      return Promise.reject(new Error('Too many files returned.'))
    }
    if (prior.attempts === 0 || prior.continuationToken != null) {
      return new Promise((resolve, reject) => {
        this.blobService.listBlobsSegmentedWithPrefix(
          this.bucket, prior.prefix, prior.continuationToken, null, (err, results) => {
            if (err) {
              return reject(err)
            }
            const totalSize = results.entries.reduce(
              (agg,x) => parseFloat(x.contentLength) + agg, 0.0)
            return resolve({ attempts: prior.attempts + 1,
                             prefix: prior.prefix,
                             totalSize: totalSize + prior.totalSize,
                             continuationToken: results.entries.continuationToken })
          })
      })
        .then(current => this.innerListBlob(current))
    } else {
      return Promise.resolve(prior.totalSize)
    }
  }

  checkUsed (prefix) {
    return this.innerListBlob({ attempts: 0, prefix, totalSize: 0, continuationToken: null })
  }

  performWrite (args) {
    // cancel write and return 402 if path is invalid
    if (! AzDriver.isPathValid(args.path)) {
      return Promise.reject(new BadPathError('Invalid Path'))
    }

    // Prepend ${address}/ to filename
    const azBlob = `${args.storageTopLevel}/${args.path}`
    const azOpts = {}

    if (this.cacheControl) {
      azOpts.contentSettings = { 'cacheControl' : this.cacheControl }
    } else {
      azOpts.contentSettings = {}
    }

    let checkPromise = Promise.resolve()
    if (this.storageLimit && this.storageLimit > 0) {
      checkPromise = this.checkUsed(`${args.storageTopLevel}`)
        .then((bytesUsed) => {
          if (this.storageLimit < bytesUsed) {
            throw new StorageLimitError(`Used ${bytesUsed}, limit: ${this.storageLimit}`)
          }
        })
    }

    azOpts.contentSettings.contentType = args.contentType

    return checkPromise
      .then(() => new Promise((resolve, reject) => {
        this.blobService.createBlockBlobFromStream(
          this.bucket, azBlob, args.stream, args.contentLength, azOpts,
          (error) => {
            // log error, reject promise.
            if (error) {
              logger.error(`failed to store ${azBlob} in container ${this.bucket}: ${error}`)
              return reject(new Error('Azure storage failure: failed failed to store' +
                                      ` ${azBlob} in container ${this.bucket}: ${error}`))
            }

            // Return success and url to user
            const readURL = this.getReadURLPrefix()
            const publicURL = `${readURL}${azBlob}`
            logger.debug(`Storing ${azBlob} in container ${this.bucket}, URL: ${publicURL}`)
            resolve(publicURL)
          })
      }))
  }
}

module.exports = AzDriver
