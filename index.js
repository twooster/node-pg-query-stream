'use strict'
var Cursor = require('pg-cursor')
var Readable = require('stream').Readable

class PgQueryStream extends Readable {
  constructor (text, values, options) {
    super(Object.assign({ objectMode: true }, options))
    this.cursor = new Cursor(text, values)
    this._reading = false
    this._closed = false
    this.batchSize = (options || {}).batchSize || 100

    // delegate Submittable callbacks to cursor
    this.handleRowDescription = this.cursor.handleRowDescription.bind(this.cursor)
    this.handleDataRow = this.cursor.handleDataRow.bind(this.cursor)
    this.handlePortalSuspended = this.cursor.handlePortalSuspended.bind(this.cursor)
    this.handleCommandComplete = this.cursor.handleCommandComplete.bind(this.cursor)
    this.handleReadyForQuery = this.cursor.handleReadyForQuery.bind(this.cursor)
    this.handleError = this.cursor.handleError.bind(this.cursor)
  }

  submit (connection) {
    this.cursor.submit(connection)
  }

  close (callback) {
    if (this._closed) {
      if (callback) {
        callback()
      }
      return
    }

    if (callback) {
      this.once('close', callback)
    }
    this.destroy()
  }

  _destroy(err, callback) {
    this._closed = true
    this.cursor.close(callback)
  }

  _read (size) {
    if (this._reading || this._closed) {
      return false
    }
    const readAmount = Math.max(size, this.batchSize)

    this._reading = true
    this.cursor.read(readAmount, (err, rows) => {
      this._reading = false

      if (this._closed) {
        return
      }

      if (err) {
        this.emit('error', err)
        return
      }

      // if we get a 0 length array we've read to the end of the cursor
      if (!rows.length) {
        this.once('end', () => this.destroy())
        this.push(null)
        return
      }

      // push each row into the stream
      for (var i = 0; i < rows.length; i++) {
        this.push(rows[i])
      }
    })
  }
}

module.exports = PgQueryStream
