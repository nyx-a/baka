
require 'logger'

module B
  # for namespace
end

class B::Log

  SEVERITY = [
    :debug,       # 0 ↑ low
    :information, # 1
    :warning,     # 2
    :error,       # 3
    :fatal,       # 4 ↓ high
  ].freeze

  def initialize(
    file,
    age:       3,
    size:      1_000_000,
    format:    '%F %T.%1N',
    separator: ' | '
  )
    @logger    = Logger.new file, age, size
    @format    = format
    @separator = separator
    @padding   = ' ' * Time.now.strftime(@format).length
    @active    = SEVERITY.to_h{ [_1, true] }
  end

  def level
    SEVERITY[ @active.select{ _2 }.keys.map{ SEVERITY.index _1 }.min ]
  end

  def level= severity
    i = SEVERITY.index severity.to_sym
    if i.nil?
      raise "invalid severity #{severity} (#{SEVERITY.join(',')})"
    else
      SEVERITY[...i].each{ @active[_1] = false } # lower
      SEVERITY[ i..].each{ @active[_1] = true  } # upper
    end
  end

  def blank
    @logger << "- #{@padding}#{@separator}\n"
  end

  def gap
    @logger << "\n"
  end

  def close
    @logger.close
  end

  def add severity, *objects
    if @active[severity.to_sym]
      s = objects.map{ _1.is_a?(String) ? _1 : _1.inspect }.join ' '
      @logger << make(letter:severity[0], message:s)
    end
    objects.one? ? objects.first : objects
  end

  def make letter:, message:, time:Time.now
    h1 = [letter.upcase,   time.strftime(@format) ].join ' '
    h2 = [letter.downcase, @padding               ].join ' '
    m  = message.gsub("\n", "\n#{h2}#{@separator}")
    [h1, @separator, m, "\n"].join
  end

  for severity in SEVERITY
    define_method severity do |*objects|
      add __method__, *objects
    end
    alias_method severity[0], severity
  end
end

