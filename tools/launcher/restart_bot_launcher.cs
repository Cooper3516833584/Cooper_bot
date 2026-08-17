using System;
using System.Diagnostics;
using System.IO;
using System.Management;
using System.Threading;

internal static class RestartBotLauncher
{
    private const string RepoDir = @"C:\Users\Cooper\Desktop\Cooper_bot";
    private const string ClientFile = "client.py";
    private const string PreferredPython = @"C:\Users\Cooper\AppData\Local\Programs\Python\Python313\python.exe";
    private const string PreferredPyLauncher = @"C:\Users\Cooper\AppData\Local\Programs\Python\Launcher\py.exe";

    private static readonly string LockPath = Path.Combine(RepoDir, "runtime", "state", "client", "client.lock");
    private static readonly string LogPath = Path.Combine(RepoDir, "runtime", "logs", "restart_bot_launcher.log");

    [STAThread]
    private static int Main(string[] args)
    {
        Console.Title = "Restart Cooper Bot";

        try
        {
            return Run(args);
        }
        catch (Exception ex)
        {
            WriteLine("FAILED: " + ex.Message);
            Log("FAILED: " + ex);
            Pause();
            return 1;
        }
    }

    private static int Run(string[] args)
    {
        bool checkOnly = HasArg(args, "--check");

        WriteLine("Restart Cooper Bot");
        WriteLine("Repo: " + RepoDir);

        if (!Directory.Exists(RepoDir))
        {
            return Fail("Repo directory does not exist.");
        }

        string python = ResolveExecutable(PreferredPython, "python.exe");
        string pyLauncher = ResolveExecutable(PreferredPyLauncher, "py.exe");
        if (python == null && pyLauncher == null)
        {
            return Fail("Python was not found.");
        }

        WriteLine("Python: " + (python ?? pyLauncher));

        if (checkOnly)
        {
            WriteLine("Check OK. No process was changed.");
            return 0;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(LogPath));
        Log("----- restart requested -----");

        StopExistingBot();
        StartBot(python, pyLauncher);

        WriteLine("Done. A new bot window should be open.");
        Thread.Sleep(1800);
        return 0;
    }

    private static void StopExistingBot()
    {
        bool stopped = StopMatchingPythonProcesses();
        if (!stopped)
        {
            stopped = StopPidFromLock();
        }

        if (stopped)
        {
            Thread.Sleep(1500);
        }
        else
        {
            WriteLine("No old bot process found.");
            Log("No old bot process found.");
        }
    }

    private static bool StopPidFromLock()
    {
        int pid;
        if (!TryReadLockPid(out pid))
        {
            return false;
        }

        try
        {
            Process process = Process.GetProcessById(pid);
            string commandLine = TryGetCommandLine(pid);
            string processName = process.ProcessName ?? "";

            if (commandLine != null)
            {
                if (!LooksLikeBotCommand(commandLine))
                {
                    WriteLine("Lock PID does not look like this bot; skipped PID " + pid + ".");
                    Log("Skipped lock PID " + pid + ": " + commandLine);
                    return false;
                }
            }
            else if (IndexOfIgnoreCase(processName, "python") < 0)
            {
                WriteLine("Lock PID is not a Python process; skipped PID " + pid + ".");
                Log("Skipped lock PID " + pid + ": process=" + processName);
                return false;
            }

            WriteLine("Stopping old bot PID " + pid + "...");
            Log("Stopping lock PID " + pid + ".");
            process.Kill();
            process.WaitForExit(5000);
            return true;
        }
        catch (ArgumentException)
        {
            Log("Lock PID " + pid + " is not running.");
            return false;
        }
        catch (Exception ex)
        {
            WriteLine("Warning: could not stop lock PID " + pid + ": " + ex.Message);
            Log("Could not stop lock PID " + pid + ": " + ex);
            return false;
        }
    }

    private static bool StopMatchingPythonProcesses()
    {
        bool stopped = false;

        try
        {
            using (ManagementObjectSearcher searcher = new ManagementObjectSearcher(
                "SELECT ProcessId, CommandLine FROM Win32_Process WHERE Name = 'python.exe' OR Name = 'pythonw.exe' OR Name = 'py.exe'"))
            {
                foreach (ManagementObject item in searcher.Get())
                {
                    string commandLine = Convert.ToString(item["CommandLine"]);
                    if (!LooksLikeBotCommand(commandLine))
                    {
                        continue;
                    }

                    int pid = Convert.ToInt32(item["ProcessId"]);
                    try
                    {
                        WriteLine("Stopping old bot PID " + pid + "...");
                        Log("Stopping matched PID " + pid + ": " + commandLine);
                        Process process = Process.GetProcessById(pid);
                        process.Kill();
                        process.WaitForExit(5000);
                        stopped = true;
                    }
                    catch (Exception ex)
                    {
                        WriteLine("Warning: could not stop PID " + pid + ": " + ex.Message);
                        Log("Could not stop matched PID " + pid + ": " + ex);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            WriteLine("Warning: could not scan Python processes: " + ex.Message);
            Log("Could not scan Python processes: " + ex);
        }

        return stopped;
    }

    private static bool TryReadLockPid(out int pid)
    {
        pid = 0;

        try
        {
            if (!File.Exists(LockPath))
            {
                return false;
            }

            string text = File.ReadAllText(LockPath).Trim();
            return int.TryParse(text, out pid) && pid > 0;
        }
        catch (Exception ex)
        {
            WriteLine("Warning: could not read lock file: " + ex.Message);
            Log("Could not read lock file: " + ex);
            return false;
        }
    }

    private static void StartBot(string python, string pyLauncher)
    {
        string fileName = python ?? pyLauncher;
        string args = python != null ? Quote(ClientFile) : "-3 " + Quote(ClientFile);

        WriteLine("Starting bot...");
        Log("Starting bot with " + fileName + " " + args);

        ProcessStartInfo startInfo = new ProcessStartInfo();
        startInfo.FileName = "cmd.exe";
        startInfo.Arguments = "/c start \"Cooper Bot\" /D " + Quote(RepoDir) + " " + Quote(fileName) + " " + args;
        startInfo.WorkingDirectory = RepoDir;
        startInfo.UseShellExecute = false;
        startInfo.CreateNoWindow = true;

        Process.Start(startInfo);
    }

    private static string TryGetCommandLine(int pid)
    {
        try
        {
            using (ManagementObjectSearcher searcher = new ManagementObjectSearcher(
                "SELECT CommandLine FROM Win32_Process WHERE ProcessId = " + pid))
            {
                foreach (ManagementObject item in searcher.Get())
                {
                    return Convert.ToString(item["CommandLine"]);
                }
            }
        }
        catch
        {
        }

        return null;
    }

    private static bool LooksLikeBotCommand(string commandLine)
    {
        if (String.IsNullOrEmpty(commandLine))
        {
            return false;
        }

        string[] parts = SplitCommandLine(commandLine);
        for (int i = 1; i < parts.Length; i++)
        {
            string part = parts[i].Trim();
            if (part.Length == 0 || part.StartsWith("-", StringComparison.Ordinal))
            {
                continue;
            }

            try
            {
                if (String.Equals(Path.GetFileName(part), ClientFile, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            catch
            {
            }
        }

        return false;
    }

    private static string[] SplitCommandLine(string commandLine)
    {
        System.Collections.Generic.List<string> parts = new System.Collections.Generic.List<string>();
        System.Text.StringBuilder current = new System.Text.StringBuilder();
        bool inQuotes = false;

        for (int i = 0; i < commandLine.Length; i++)
        {
            char c = commandLine[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (Char.IsWhiteSpace(c) && !inQuotes)
            {
                if (current.Length > 0)
                {
                    parts.Add(current.ToString());
                    current.Length = 0;
                }
                continue;
            }

            current.Append(c);
        }

        if (current.Length > 0)
        {
            parts.Add(current.ToString());
        }

        return parts.ToArray();
    }

    private static string ResolveExecutable(string preferredPath, string pathName)
    {
        if (!String.IsNullOrEmpty(preferredPath) && File.Exists(preferredPath))
        {
            return preferredPath;
        }

        string path = Environment.GetEnvironmentVariable("PATH") ?? "";
        string[] parts = path.Split(Path.PathSeparator);
        for (int i = 0; i < parts.Length; i++)
        {
            string candidate = Path.Combine(parts[i].Trim(), pathName);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return null;
    }

    private static bool HasArg(string[] args, string expected)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (String.Equals(args[i], expected, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static int Fail(string message)
    {
        WriteLine("FAILED: " + message);
        Log("FAILED: " + message);
        Pause();
        return 1;
    }

    private static void Pause()
    {
        WriteLine("Press Enter to close.");
        try
        {
            Console.ReadLine();
        }
        catch
        {
        }
    }

    private static void WriteLine(string message)
    {
        Console.WriteLine(message);
    }

    private static void Log(string message)
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(LogPath));
            File.AppendAllText(LogPath, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + message + Environment.NewLine);
        }
        catch
        {
        }
    }

    private static string Quote(string value)
    {
        return "\"" + value.Replace("\"", "\\\"") + "\"";
    }

    private static int IndexOfIgnoreCase(string source, string value)
    {
        return source.IndexOf(value, StringComparison.OrdinalIgnoreCase);
    }
}
