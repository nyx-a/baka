
module B
  # namespace
end

class B::Structure
  def initialize **hash
    for k,v in hash
      setter = "#{k}=".to_sym
      if respond_to? setter
        self.send setter, v
      else
        raise KeyError, "No such element `#{k}`"
      end
    end
  end
end

