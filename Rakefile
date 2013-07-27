require "bundler/gem_tasks"
require "active_support/core_ext"

task "sync:go" do
  Dir.glob("redis/*") do |src|
    script = File.read(src)

    name = /redis\/(.*)\.lua/.match(src)[1]

    File.open("go/#{name}.go", "w") do |dest|  
      dest.puts <<-EOF
package fairway

func #{name.camelize}() string {
  return `
#{script}
`
}
      EOF
    end  
  end
end
