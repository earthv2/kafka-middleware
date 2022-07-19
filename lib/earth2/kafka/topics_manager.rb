# frozen_string_literal: true

class TopicsManager
  attr_reader :classes

  def initialize(classes)
    @classes = classes
  end

  def sync
    classes.each do |klass|
      klass.topics.each do |name, topic|
      end
    end
  end
end
