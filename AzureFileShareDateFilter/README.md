# Azure Files date filter (.NET 10)

1. Install .NET 10 SDK.
2. Edit `appsettings.json` with the share name and date range.
3. Set `AZURE_STORAGE_CONNECTION_STRING` environment variable (recommended), or set `ConnectionString` in appsettings.json. Never commit secrets.
4. Run `dotnet restore` and `dotnet run`.

The date range is inclusive in UTC and applies to Azure Files `LastModified` timestamps. Files in subdirectories are included when `Recursive` is true. `DirectoryPath` may be blank or a slash-separated folder path. This lists files only; it does not download them.
