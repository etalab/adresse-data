#!/usr/bin/env node
require('dotenv').config()

const {join, basename} = require('path')
const {pipeline} = require('stream')
const {promisify} = require('util')
const {createReadStream, createWriteStream} = require('fs')
const {ensureFile, readdir, ensureDir, copy, symlink, remove} = require('fs-extra')
const tar = require('tar-stream')

const pump = promisify(pipeline)

const {SOURCES_DIR, ARCHIVES_DIR} = process.env
const OUTPUT_DIR = join(__dirname, '..', 'dist')

const FILETYPE_MAPPING = {
  ban: 'ban',
  adresse_id_ign: 'housenumber-id-ign',
  group_id_ign: 'group-id-ign'
}

function getFileInfo(fileName) {
  const result = fileName.match(/^(ban|adresse_id_ign|group_id_ign)_(\w{2,3})_(\d{8})\.csv\.gz$/)
  if (result) {
    const [, fileType, departement] = result
    return {fileType: FILETYPE_MAPPING[fileType], departement}
  }
}

function getFilePath(date, fileType, departement) {
  return join(OUTPUT_DIR, date, fileType, `${fileType}-${departement}.csv.gz`)
}

async function listArchives(archivesPath) {
  const files = await readdir(archivesPath)
  return files.filter(f => f.match(/^\d{8}\.tar$/)).map(f => join(archivesPath, f))
}

async function extractArchive(archivePath) {
  const fileName = basename(archivePath)
  const [, year, month, day] = fileName.match(/^(\d{4})(\d{2})(\d{2}).tar$/)
  const date = `${year}-${month}-${day}`

  const extract = tar.extract()

  extract.on('entry', async (header, stream, next) => {
    const fileName = basename(header.name)
    const fileInfo = getFileInfo(fileName)

    stream.on('end', () => next())

    if (!fileInfo) {
      console.error(`Fichier inconnu : ${fileName}`)
      stream.resume()
      return
    }

    const {fileType, departement} = fileInfo
    const outputFilePath = getFilePath(date, fileType, departement)

    await ensureFile(outputFilePath)

    stream.pipe(createWriteStream(outputFilePath))
  })

  await pump(createReadStream(archivePath), extract)
}

async function electLatest() {
  const files = await readdir(OUTPUT_DIR)
  const dates = files.filter(f => f.match(/^\d{4}-\d{2}-\d{2}$/)).sort()
  const latest = dates[dates.length - 1]
  await remove(join(OUTPUT_DIR, 'latest'))
  await symlink(latest, join(OUTPUT_DIR, 'latest'))
}

async function main() {
  const archivesPaths = await listArchives(SOURCES_DIR)

  await Promise.all(archivesPaths.map(async archivePath => {
    await extractArchive(archivePath)
    await ensureDir(ARCHIVES_DIR)
    await copy(archivePath, join(ARCHIVES_DIR, basename(archivePath)))
  }))

  await electLatest()
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
