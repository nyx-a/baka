
def hm(...)
  B::HM.new(...)
end

module B
  # namespace
end

class B::HM
  attr_reader :r

  def initialize *args
    @r = self.class.rationalize args.one? ? args.first : args
  end

  def inspect
    self.class.r_to_a(@r).join ':'
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

  def self.a_to_r *array
    array.reverse.inject do |s,b|
      Rational(s, 60) + Rational(b)
    end
  end

  def self.r_to_a rational
    array = [ ]
    r = rational
    until r.zero?
      truncate  = r.truncate
      remainder = r - truncate
      array.push truncate
      r = remainder.numerator * Rational(60, remainder.denominator)
    end
    return array
  end

  def self.rationalize other
    case other
    when self   then other.r
    when Array  then a_to_r(*other)
    when String then a_to_r(*other.split(':').map(&:to_f))
    else
      if other.respond_to? :rationalize
        other.rationalize
      else
        raise ArgumentError, "cannot rationalize => `#{other.inspect}`"
      end
    end
  end
end

