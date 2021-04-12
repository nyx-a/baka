
require 'openssl'
require 'mongo'
require 'ruby-filemagic'
require 'mime-types'

class BlobApartment

  #
  #* class methods
  #

  class << self
    def make_instance(
      collection:, server:, database:, user:, password:, auth:nil
    )
      new(
        client: Mongo::Client.new(
          [server],
          database:    database,
          user:        user,
          password:    password,
          auth_source: auth,
        ),
        collection: collection
      )
    end

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
    @possession = @client[collection + '.possessions']
    @bucket = Mongo::Grid::FSBucket.new @client, fs_name:collection
  end

  #
  #* upload / download
  #
  # 全く同じデータをほぼ同時にuploadした場合重複が起こり得る
  # 全く同じデータであるならば重複しても問題はない
  #

  # BSON::Binary(sha256) ... first insert
  # nil                  ... already has that blob
  def upload blob
    sha256str  = BlobApartment.sha256 blob
    sha256bson = BlobApartment.string_to_bson sha256str
    mimetype   = BlobApartment.mimetype blob
    if register sha256bson
      @bucket.upload_from_stream(
        sha256str,
        blob,
        metadata:{
          sha256:   sha256bson,
          mimetype: mimetype,
        }
      )
      return sha256bson
    end
  end

  def upload_from_file path
    open(path, 'rb') do |fin|
      upload fin.read
    end
  end

  def download target, to=nil
    doc = sole_completion target
    if doc
      to = '' if to.nil?
      @bucket.download_to_stream doc[:id], to
      return to
    end
  end

  def download_to_file target, path=nil
    doc = sole_completion target
    if doc
      if path.nil?
        path = BlobApartment.bson_to_string doc[:sha256]
        sfx  = BlobApartment.suffix_for doc[:mimetype]
        path += ".#{sfx}" if sfx
      end
      open(path, 'wb') do |fout|
        @bucket.download_to_stream doc[:id], fout
      end
      return true
    end
  end

  #
  #* find
  #

  def list mimetype:nil, digest:nil, limit:nil
    if digest.is_a? String
      digest = BlobApartment.string_to_bson digest
    end
    query  = { }
    option = { }
    query.update 'metadata.mimetype':mimetype if mimetype
    query.update 'metadata.sha256':digest if digest
    option.update 'limit':limit if limit
    @bucket.find(query, option).map{ BlobApartment.bson_to_h _1 }
  end

  def sole_completion target
    case target
    when BSON::ObjectId
      doc = @bucket.find({_id:target}, {limit:1}).first
      if doc
        BlobApartment.bson_to_h doc
      end
    when BSON::Binary, String
      list(digest:target, limit:1)&.first
    else
      raise TypeError, "invalid class for target => #{target.class}"
    end
  end

  def digest? digest
    @possession.find({_id:digest}, {limit:1}).count == 1
  end

  def data? data
    digest? BlobApartment.sha256_in_bson data
  end

  def has_file? path
    buffer     = open(path, 'rb').read
    length     = buffer.length
    sha256bson = BlobApartment.sha256_in_bson buffer
    mimetype   = BlobApartment.mimetype buffer
    d = sole_completion sha256bson
    (d and d[:length]==length and d[:mimetype]==mimetype)
  end

  #
  #* consistency check
  #

  def doctor
    count = 0
    for i in @bucket.find
      sha256bson = i[:metadata][:sha256]
      buffer = ''
      @bucket.download_to_stream i[:_id], buffer
      twice = BlobApartment.sha256_in_bson buffer

      if i[:length] != buffer.length
        puts "ERROR: Inconsistencies in size"
        p i
      end
      if sha256bson != twice
        puts "ERROR: Inconsistencies in digest"
        p i
      end
      if BlobApartment.bson_to_string(twice) != i[:filename]
        puts "ERROR: Inconsistencies in filename"
        p i
      end
      count += 1
    end
    count
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

