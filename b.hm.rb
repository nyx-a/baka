
def hm(...)
  B::HM.new(...)
end

module B
  # namespace
end

class B::HM
  attr_reader :r

  def initialize *args
    case args.size
    when 2
      @r = self.class.hm_to_r(*args)
    when 1
      @r = self.class.rationalize args.first
    else
      raise ArgumentError, "wrong number of arguments (given #{args.size})"
    end
  end

  def inspect
    self.class.r_to_hm( @r ).join ':'
  end

  def + other
    B::HM.new @r + self.class.rationalize(other)
  end

  def - other
    B::HM.new @r - self.class.rationalize(other)
  end

  def * other
    B::HM.new @r * other
  end

  def / other
    B::HM.new @r / other
  end

  #- - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  def self.hm_to_r h, m
    h + Rational(m, 60)
  end

  # r : Rational を60進数に見立てて [商, 余] を返す
  def self.r_to_hm r
    truncate  = r.truncate
    remainder = r - truncate
    [
      truncate,
      (remainder.numerator * Rational(60, remainder.denominator)).to_f
    ]
  end

  def self.rationalize other
    if other.is_a? self
      other.r
    elsif other.respond_to? :rationalize
      other.rationalize
    else
      raise ArgumentError, "cannot rationalize => `#{other.inspect}`"
    end
  end
end

