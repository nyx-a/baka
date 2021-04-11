
require 'openssl'
require 'mongo'
require 'ruby-filemagic'
require 'mime-types'

class BlobApartment

  #
  #* class methods
  #

  class << self
    def mimetype blob
      FileMagic.fm(FileMagic::MAGIC_MIME_TYPE).buffer blob
    end

    def suffix_for mimetype
      MIME::Types[mimetype].first&.extensions&.first
    end

    def sha256 blob
      Digest::SHA256.hexdigest blob
    end

    def sha256_in_bson blob
      string_to_bson sha256 blob
    end

    # BSON::Binary to String
    def bson_to_string b
      b.data.bytes.map{ '%02x' % _1 }.join
    end

    # String to BSON::Binary
    def string_to_bson s
      BSON::Binary.new s.scan(/../).map{ _1.to_i(16).chr }.join
    end

    # BSON::Document to Hash (for gridFS bucket)
    def bson_to_h bsondoc
      {
        id:       bsondoc[:_id],
        length:   bsondoc[:length],
        date:     bsondoc[:uploadDate],
        sha256:   bsondoc[:metadata][:sha256],
        mimetype: bsondoc[:metadata][:mimetype],
      }
    end
  end

  #
  #* instance methods
  #

  def initialize client:, collection:
    @client = client
    @possession = @client[collection + '.digests']
    @bucket = Mongo::Grid::FSBucket.new @client, fs_name:collection
  end

  #
  #* upload / download
  #

  def upload blob
    sha256_s = BlobApartment.sha256 blob
    sha256_b = BlobApartment.string_to_bson sha256_s
    mimetype = BlobApartment.mimetype blob
    if register sha256_b
      @bucket.upload_from_stream(
        sha256_s,
        blob,
        metadata:{
          sha256:   sha256_b,
          mimetype: mimetype,
        }
      )
      return sha256_b
    end
  end

  def upload_from_file path
    open(path, 'rb') do |fin|
      upload fin.read
    end
  end

  def download digest, to:nil
    doc = find_doc_with_digest digest
    if doc
      to = '' if to.nil?
      @bucket.download_to_stream doc[:_id], to
      return to
    end
  end

  def download_into_file digest, path:nil
    doc = find_doc_with_digest digest
    if doc
      if path.nil?
        f = doc[:filename]
        s = BlobApartment.suffix_for doc[:metadata][:mimetype]
        path = "#{f}.#{s}"
      end
      open(path, 'wb') do |fout|
        @bucket.download_to_stream doc[:_id], fout
      end
      return true
    end
  end

  #
  #* find
  #

  def find_docs(...)
    @bucket.find(...).map{ BlobApartment.bson_to_h _1 }
  end

  def find_docs_with_type mimetype
    @bucket
      .find('metadata.mimetype':mimetype)
      .map{ BlobApartment.bson_to_h _1 }
  end

  def find_doc_with_digest digest
    doc = @bucket.find({'metadata.sha256':digest}, {limit:1}).first
    BlobApartment.bson_to_h doc
  end

  def digest? digest
    @possession.find({_id:digest}, {limit:1}).count == 1
  end

  def data? data
    digest? BlobApartment.sha256_in_bson data
  end

  private

  # true  ... First insert
  # false ... Already have the digest (No insertion)
  def register digest
    before = @possession.find_one_and_update(
      { _id: digest },
      { }, # update nothing
      {
        upsert: true,
        return_document: :before,
      }
    )
    return before.nil?
  end
end

