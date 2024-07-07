#!/bin/bash
set -euo pipefail

dotnet new sln --force -n yamux
dotnet sln add ./src/**/*.csproj
dotnet sln add ./test/**/*.csproj
