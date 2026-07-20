# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name          = "glyphastore"
  spec.version       = begin
    require_relative "lib/glypha_store/version"
    GlyphaStore::VERSION
  end
  spec.authors       = ["GlyphaStore maintainers"]
  spec.email         = ["maintainers@glyphastore.dev"]

  spec.summary       = "Official Ruby client for GlyphaStore wire protocol v2"
  spec.description   = "Native isomorphic TCP client: pipelines, batch, structured errors, " \
                       "monotonic deadlines, and committed/rejected/indeterminate mutations."
  spec.homepage      = "https://github.com/gpicchiarelli/GlyphaStore"
  spec.license       = "BSD-3-Clause"
  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/gpicchiarelli/GlyphaStore/tree/main/sdk/ruby"
  spec.metadata["changelog_uri"] = "https://github.com/gpicchiarelli/GlyphaStore/blob/main/sdk/ruby/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "https://github.com/gpicchiarelli/GlyphaStore/issues"
  spec.metadata["documentation_uri"] = "https://github.com/gpicchiarelli/GlyphaStore/blob/main/sdk/ruby/README.md"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").select do |f|
      f.start_with?("lib/", "exe/") || %w[LICENSE README.md CHANGELOG.md PACKAGING.md].include?(f)
    end
  rescue StandardError
    Dir["lib/**/*", "exe/*", "LICENSE", "README.md", "CHANGELOG.md", "PACKAGING.md"]
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Optional Fiber client: require "glypha_store/async_client"
  spec.add_development_dependency "async", "~> 2.0"
  spec.add_development_dependency "minitest", "~> 5.0"
end
