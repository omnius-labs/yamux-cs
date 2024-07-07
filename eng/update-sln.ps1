dotnet new sln --force -n yamux
dotnet sln add (ls -r ./src/**/*.csproj)
dotnet sln add (ls -r ./test/**/*.csproj)
