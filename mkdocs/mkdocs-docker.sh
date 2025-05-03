#!/bin/bash

case "$1" in
  "serve")
    docker run --rm -it -p 8000:8000 -v ${PWD}:/docs squidfunk/mkdocs-material serve -a 0.0.0.0:8000
    ;;
  "build")
    docker run --rm -it -v ${PWD}:/docs squidfunk/mkdocs-material build
    ;;
  *)
    echo "Usage: $0 {serve|build}"
    echo "  serve: Start development server"
    echo "  build: Build static site"
    exit 1
    ;;
esac