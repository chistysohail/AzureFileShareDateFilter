using Azure;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Text;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var settings = configuration.GetSection("AzureFileShare").Get<FileShareSettings>()
    ?? throw new InvalidOperationException("Missing AzureFileShare settings.");

var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING")
    ?? settings.ConnectionString;
if (string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(settings.ShareName))
    throw new InvalidOperationException("Set the storage connection string and share name.");

var start = DateOnly.ParseExact(settings.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
var end = DateOnly.ParseExact(settings.EndDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
if (end < start) throw new ArgumentException("EndDate must be on or after StartDate.");
var fromUtc = new DateTimeOffset(start.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
var untilUtcExclusive = new DateTimeOffset(end.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);

var share = new ShareClient(connectionString, settings.ShareName);
var root = share.GetRootDirectoryClient();
var directory = root;
var basePath = settings.DirectoryPath.Trim('/');
foreach (var segment in basePath.Split('/', StringSplitOptions.RemoveEmptyEntries))
    directory = directory.GetSubdirectoryClient(segment);

var operation = configuration.GetSection("Operation").Get<OperationSettings>() ?? new OperationSettings();
if (!Enum.TryParse<OperationMode>(operation.Mode, true, out var mode) ||
    !Enum.IsDefined(mode))
    throw new ArgumentException("Operation:Mode must be List or Download.");
if (mode == OperationMode.Download && string.IsNullOrWhiteSpace(operation.DownloadDirectory))
    throw new ArgumentException("Operation:DownloadDirectory is required for Download mode.");
var downloadRoot = mode == OperationMode.Download ? Path.GetFullPath(operation.DownloadDirectory) : "";

var output = configuration.GetSection("Output").Get<OutputSettings>() ?? new OutputSettings();
StreamWriter? writer = null;
string? outputPath = null;
if (output.SaveToFile)
{
    var outputDirectory = Path.GetFullPath(output.Directory);
    Directory.CreateDirectory(outputDirectory);
    // CreateNew guarantees no overwrite even when multiple runs begin in the same second.
    var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
    for (int suffix = 0; ; suffix++)
    {
        var name = $"MatchingFiles_{timestamp}{(suffix == 0 ? "" : $"_{suffix}")}.txt";
        var candidate = Path.Combine(outputDirectory, name);
        try
        {
            writer = new StreamWriter(new FileStream(candidate, FileMode.CreateNew, FileAccess.Write, FileShare.Read), new UTF8Encoding(false));
            outputPath = candidate;
            break;
        }
        catch (IOException) when (File.Exists(candidate)) { }
    }
}

void Print(string message)
{
    Console.WriteLine(message);
    writer?.WriteLine(message);
}

int count = 0;
int downloaded = 0;
int skipped = 0;
int failed = 0;
try
{
    Print($"Mode: {mode}; Share: {settings.ShareName}; UTC modified date: {start} through {end}");
    Print($"Directory: {(string.IsNullOrEmpty(basePath) ? "(root)" : basePath)}; Recursive: {settings.Recursive}");
    await ScanAsync(directory, basePath);
    Print($"Total matching files: {count}; Downloaded: {downloaded}; Skipped: {skipped}; Failed downloads: {failed}");
}
finally
{
    if (writer is not null) await writer.DisposeAsync();
}
if (outputPath is not null) Console.WriteLine($"Saved results to: {outputPath}");

async Task ScanAsync(ShareDirectoryClient current, string path)
{
    try
    {
        await foreach (ShareFileItem item in current.GetFilesAndDirectoriesAsync())
        {
            var childPath = string.IsNullOrEmpty(path) ? item.Name : $"{path}/{item.Name}";
            if (item.IsDirectory)
            {
                if (settings.Recursive)
                    await ScanAsync(current.GetSubdirectoryClient(item.Name), childPath);
                continue;
            }

            try
            {
                var file = current.GetFileClient(item.Name);
                var properties = (await file.GetPropertiesAsync()).Value;
                var modified = properties.LastModified;
                if (modified >= fromUtc && modified < untilUtcExclusive)
                {
                    count++;
                    Print($"{modified:yyyy-MM-dd HH:mm:ss} UTC | {properties.ContentLength,12} bytes | {childPath}");
                    if (mode == OperationMode.Download)
                        await DownloadMatchingFileAsync(file, childPath);
                }
            }
            catch (RequestFailedException ex)
            {
                Console.Error.WriteLine($"Could not inspect {childPath}: {ex.Message}");
            }
        }
    }
    catch (RequestFailedException ex)
    {
        Console.Error.WriteLine($"Could not list directory '{path}': {ex.Message}");
    }
}

async Task DownloadMatchingFileAsync(ShareFileClient file, string shareRelativePath)
{
    // Retain the path below the configured DirectoryPath, not the entire share path.
    var relative = string.IsNullOrEmpty(basePath)
        ? shareRelativePath
        : shareRelativePath[(basePath.Length + 1)..];
    var parts = relative.Split('/');
    if (parts.Any(x => x is "" or "." or ".." || x.Contains('\\') || x.Contains(':')))
    {
        failed++;
        Print($"DOWNLOAD FAILED (unsafe path): {shareRelativePath}");
        return;
    }

    // Flattening can produce duplicate names; CreateNew will skip rather than overwrite.
    var targetParts = operation.PreserveFolderStructure ? parts : new[] { parts[^1] };
    var target = Path.Combine(new[] { downloadRoot }.Concat(targetParts).ToArray());
    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
    try
    {
        // Always CreateNew, preventing overwrite or truncation of existing files.
        // OverwriteExisting=true explicitly deletes an existing local file first.
        if (operation.OverwriteExisting && File.Exists(target)) File.Delete(target);
        await using (var stream = new FileStream(target, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
            try
            {
                var response = await file.DownloadAsync();
                await using var azureStream = response.Value.Content;
                await azureStream.CopyToAsync(stream);
            }
            catch
            {
                await stream.DisposeAsync();
                File.Delete(target); // clean up only the incomplete file created by this run
                throw;
            }
        }
        downloaded++;
        Print($"DOWNLOADED: {target}");
    }
    catch (IOException) when (File.Exists(target) && !operation.OverwriteExisting)
    {
        skipped++;
        Print($"SKIPPED (already exists): {target}");
    }
    catch (Exception ex) when (ex is RequestFailedException or IOException or UnauthorizedAccessException)
    {
        failed++;
        Print($"DOWNLOAD FAILED: {shareRelativePath} | {ex.Message}");
    }
}

sealed class FileShareSettings
{
    public string ConnectionString { get; set; } = "";
    public string ShareName { get; set; } = "";
    public string DirectoryPath { get; set; } = "";
    public string StartDate { get; set; } = "";
    public string EndDate { get; set; } = "";
    public bool Recursive { get; set; } = true;
}


sealed class OutputSettings
{
    public bool SaveToFile { get; set; } = true;
    public string Directory { get; set; } = "Output";
}


enum OperationMode { List, Download }
sealed class OperationSettings
{
    public string Mode { get; set; } = "List";
    public string DownloadDirectory { get; set; } = "Downloads";
    public bool PreserveFolderStructure { get; set; } = true;
    public bool OverwriteExisting { get; set; } = false;
}
