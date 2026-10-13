using Azure;
using Azure.Core;
using Azure.Storage.Files.Shares;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Channels;

var config = new ConfigurationBuilder().SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false).Build();
var settings = config.GetSection("AzureFileShare").Get<FileShareSettings>()
    ?? throw new InvalidOperationException("Missing AzureFileShare settings.");
var operation = config.GetSection("Operation").Get<OperationSettings>() ?? new();
var output = config.GetSection("Output").Get<OutputSettings>() ?? new();
var connection = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
if (string.IsNullOrWhiteSpace(connection)) connection = settings.ConnectionString;
if (string.IsNullOrWhiteSpace(connection) || string.IsNullOrWhiteSpace(settings.ShareName))
    throw new ArgumentException("Configure Azure storage connection string and share name.");
if (!Enum.TryParse<OperationMode>(operation.Mode, true, out var mode) || !Enum.IsDefined(mode))
    throw new ArgumentException("Operation:Mode must be List or Download.");
if (operation.MaxConcurrentDownloads is < 1 or > 64)
    throw new ArgumentOutOfRangeException(nameof(operation.MaxConcurrentDownloads), "Use 1 through 64.");
if (mode == OperationMode.Download && string.IsNullOrWhiteSpace(operation.DownloadDirectory))
    throw new ArgumentException("DownloadDirectory is required in Download mode.");
var start = DateOnly.ParseExact(settings.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
var end = DateOnly.ParseExact(settings.EndDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
if (end < start || end == DateOnly.MaxValue) throw new ArgumentException("Invalid date range.");
var fromUtc = new DateTimeOffset(start.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
var toUtcExclusive = new DateTimeOffset(end.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
var basePath = (settings.DirectoryPath ?? "").Replace('\\', '/').Trim('/');
var segments = basePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
if (segments.Any(s => s is "." or ".." || s.Contains(':')))
    throw new ArgumentException("Invalid Azure directory path.");
var downloadRoot = mode == OperationMode.Download ? Path.GetFullPath(operation.DownloadDirectory) : "";
var clientOptions = new ShareClientOptions();
clientOptions.Retry.Mode = RetryMode.Exponential;
clientOptions.Retry.MaxRetries = 5;
clientOptions.Retry.Delay = TimeSpan.FromSeconds(1);
clientOptions.Retry.MaxDelay = TimeSpan.FromSeconds(15);
var share = new ShareClient(connection, settings.ShareName, clientOptions);
var directory = share.GetRootDirectoryClient();
foreach (var segment in segments) directory = directory.GetSubdirectoryClient(segment);

StreamWriter? writer = null;
string? reportPath = null;
if (output.SaveToFile)
{
    var reportDir = Path.GetFullPath(output.Directory);
    Directory.CreateDirectory(reportDir);
    var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
    for (var suffix = 0; ; suffix++)
    {
        var candidate = Path.Combine(reportDir, $"MatchingFiles_{stamp}{(suffix == 0 ? "" : $"_{suffix}")}.txt");
        try
        {
            writer = new StreamWriter(new FileStream(candidate, FileMode.CreateNew, FileAccess.Write, FileShare.Read), new UTF8Encoding(false)) { AutoFlush = true };
            reportPath = candidate;
            break;
        }
        catch (IOException) when (File.Exists(candidate)) { }
    }
}
var printLock = new object();
void Print(string line)
{
    lock (printLock)
    {
        Console.WriteLine(line);
        writer?.WriteLine(line);
    }
}
var matched = 0;
var downloaded = 0;
var skipped = 0;
var failed = 0;
long bytesDownloaded = 0;
var stopwatch = Stopwatch.StartNew();
var queue = Channel.CreateBounded<DownloadJob>(new BoundedChannelOptions(Math.Max(16, operation.MaxConcurrentDownloads * 4))
{
    FullMode = BoundedChannelFullMode.Wait,
    SingleWriter = true,
    SingleReader = false
});

try
{
    Print($"Mode: {mode} | Share: {settings.ShareName} | Directory: {(basePath.Length == 0 ? "(root)" : basePath)}");
    Print($"Modified UTC: {start:yyyy-MM-dd} through {end:yyyy-MM-dd} | Recursive: {settings.Recursive}");
    if (mode == OperationMode.Download)
        Print($"Download root: {downloadRoot} | Parallel workers: {operation.MaxConcurrentDownloads}");

    var workers = mode == OperationMode.Download
        ? Enumerable.Range(0, operation.MaxConcurrentDownloads)
            .Select(_ => DownloadWorkerAsync()).ToArray()
        : Array.Empty<Task>();
    try
    {
        await ScanAsync(directory, basePath);
    }
    finally
    {
        queue.Writer.TryComplete();
        await Task.WhenAll(workers);
    }
    stopwatch.Stop();
    Print($"Matching: {matched} | Downloaded: {downloaded} | Skipped: {skipped} | Failed: {failed}");
    Print($"Bytes downloaded: {bytesDownloaded:N0} | Elapsed: {stopwatch.Elapsed:hh\\:mm\\:ss}");
}
finally
{
    if (writer is not null) await writer.DisposeAsync();
}
if (reportPath is not null) Console.WriteLine($"Report saved: {reportPath}");

async Task ScanAsync(Azure.Storage.Files.Shares.ShareDirectoryClient current, string path)
{
    try
    {
        await foreach (var item in current.GetFilesAndDirectoriesAsync())
        {
            var childPath = string.IsNullOrEmpty(path) ? item.Name : $"{path}/{item.Name}";
            if (item.IsDirectory)
            {
                if (settings.Recursive) await ScanAsync(current.GetSubdirectoryClient(item.Name), childPath);
                continue;
            }
            try
            {
                var file = current.GetFileClient(item.Name);
                var properties = (await file.GetPropertiesAsync()).Value;
                if (properties.LastModified < fromUtc || properties.LastModified >= toUtcExclusive) continue;
                Interlocked.Increment(ref matched);
                Print($"{properties.LastModified:yyyy-MM-dd HH:mm:ss} UTC | {properties.ContentLength,12:N0} bytes | {childPath}");
                if (mode == OperationMode.Download)
                    await queue.Writer.WriteAsync(new DownloadJob(file, childPath));
            }
            catch (RequestFailedException ex)
            {
                Interlocked.Increment(ref failed);
                Print($"INSPECTION FAILED: {childPath} | {ex.Message}");
            }
        }
    }
    catch (RequestFailedException ex)
    {
        Interlocked.Increment(ref failed);
        Print($"LISTING FAILED: {path} | {ex.Message}");
    }
}

async Task DownloadWorkerAsync()
{
    await foreach (var job in queue.Reader.ReadAllAsync())
    {
        try { await DownloadOneAsync(job); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or RequestFailedException or ArgumentException)
        {
            Interlocked.Increment(ref failed);
            Print($"DOWNLOAD FAILED: {job.Path} | {ex.Message}");
        }
    }
}

async Task DownloadOneAsync(DownloadJob job)
{
    var relative = basePath.Length == 0 ? job.Path : job.Path[(basePath.Length + 1)..];
    var parts = relative.Split('/');
    if (parts.Any(p => p.Length == 0 || p is "." or ".." || p.Contains('\\') || p.Contains(':') || p.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0))
        throw new ArgumentException("Unsafe filename or path.");
    var targetParts = operation.PreserveFolderStructure ? parts : new[] { parts[^1] };
    var target = Path.GetFullPath(Path.Combine(new[] { downloadRoot }.Concat(targetParts).ToArray()));
    if (!target.StartsWith(downloadRoot.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
        throw new ArgumentException("Target escapes download directory.");
    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
    if (!operation.OverwriteExisting && File.Exists(target))
    {
        Interlocked.Increment(ref skipped);
        Print($"SKIPPED (exists): {target}");
        return;
    }

    // A unique temporary file prevents incomplete files from appearing under their final names.
    var temp = target + "." + Guid.NewGuid().ToString("N") + ".part";
    try
    {
        await using (var destination = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, useAsync: true))
        {
            var response = await job.Client.DownloadAsync();
            await using var source = response.Value.Content;
            await source.CopyToAsync(destination, 128 * 1024);
        }
        try
        {
            File.Move(temp, target, overwrite: operation.OverwriteExisting);
        }
        catch (IOException) when (!operation.OverwriteExisting && File.Exists(target))
        {
            Interlocked.Increment(ref skipped);
            Print($"SKIPPED (exists): {target}");
            return;
        }
        var length = new FileInfo(target).Length;
        Interlocked.Add(ref bytesDownloaded, length);
        Interlocked.Increment(ref downloaded);
        Print($"DOWNLOADED: {target} ({length:N0} bytes)");
    }
    finally
    {
        if (File.Exists(temp)) File.Delete(temp);
    }
}

record DownloadJob(Azure.Storage.Files.Shares.ShareFileClient Client, string Path);
sealed class FileShareSettings
{
    public string ConnectionString { get; set; } = "";
    public string ShareName { get; set; } = "";
    public string DirectoryPath { get; set; } = "";
    public string StartDate { get; set; } = "";
    public string EndDate { get; set; } = "";
    public bool Recursive { get; set; } = true;
}
enum OperationMode { List, Download }
sealed class OperationSettings
{
    public string Mode { get; set; } = "List";
    public string DownloadDirectory { get; set; } = "Downloads";
    public bool PreserveFolderStructure { get; set; } = true;
    public bool OverwriteExisting { get; set; } = false;
    public int MaxConcurrentDownloads { get; set; } = 8;
}
sealed class OutputSettings
{
    public bool SaveToFile { get; set; } = true;
    public string Directory { get; set; } = "Output";
}
