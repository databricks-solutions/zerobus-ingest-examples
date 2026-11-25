// SDK matrix client, .NET (Databricks.Zerobus.Ingest.Sdk from the monorepo's dotnet/, over the Rust core FFI).
// Usage: SdkMatrixClient <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC
// Prints: ZB_RESULT {"ok": bool, "error": str, "acked": int}
using System.Text;
using Databricks.Zerobus;

namespace SdkMatrixClient;

public static class Program
{
    private const int N = 20;
    private const int MismatchAt = 5;

    public static int Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("usage: SdkMatrixClient <format> <scenario> --table ...");
            return 2;
        }
        var format = args[0];
        var scenario = args[1];
        var opts = ParseOptions(args, 2);

        var ok = false;
        var error = "";
        var acked = 0;
        try
        {
            acked = Run(format, scenario == "mismatch", opts);
            ok = true;
        }
        catch (ZerobusException e)
        {
            error = e.RawMessage;
        }
        catch (Exception e)
        {
            error = e.Message;
        }
        Console.WriteLine($"ZB_RESULT {{\"ok\": {(ok ? "true" : "false")}, \"error\": \"{Escape(error)}\", \"acked\": {acked}}}");
        return 0;
    }

    private static Dictionary<string, string> ParseOptions(string[] args, int from)
    {
        var opts = new Dictionary<string, string>();
        for (var i = from; i + 1 < args.Length; i += 2)
        {
            opts[args[i][2..]] = args[i + 1];
        }
        return opts;
    }

    private static int Run(string format, bool mismatch, Dictionary<string, string> o)
    {
        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(o["endpoint"])
            .UnityCatalogUrl(o["uc"])
            .Build();
        var table = o["table"];
        var id = o["client-id"];
        var secret = o["client-secret"];
        var acked = 0;
        if (format == "json")
        {
            using var stream = sdk.CreateJsonStream(table, id, secret);
            for (var i = 0; i < N; i++)
            {
                stream.IngestRecord(JsonRecord(i, mismatch));
                acked++;
            }
            stream.Flush();
            stream.Close();
        }
        else if (format == "proto")
        {
            using var stream = sdk.CreateProtoStream(table, ProtoDescriptor(mismatch), id, secret);
            for (var i = 0; i < N; i++)
            {
                stream.IngestRecord(ProtoRecord(i));
                acked++;
            }
            stream.Flush();
            stream.Close();
        }
        else
        {
            throw new InvalidOperationException($"format {format} is not supported by the .NET SDK");
        }
        return acked;
    }

    private static string JsonRecord(int i, bool mismatch)
    {
        var extra = mismatch && i == MismatchAt ? ",\"region\":\"eu\"" : "";
        return $"{{\"device_name\":\"m-{i}\",\"temp\":{i},\"humidity\":2{extra}}}";
    }

    // The proto descriptor for the table, built by hand so the client needs no protobuf package.
    // DescriptorProto: name=1, field=2 (repeated). FieldDescriptorProto: name=1, number=3, label=4 (1 = optional), type=5.
    private static byte[] ProtoDescriptor(bool mismatch)
    {
        const int kString = 9, kInt32 = 5, kInt64 = 3;
        var d = new List<byte>();
        PutStr(d, 1, "SdkMatrix");
        AddField(d, "device_name", 1, kString);
        AddField(d, "temp", 2, kInt32);
        AddField(d, "humidity", 3, kInt64);
        if (mismatch)
        {
            AddField(d, "region", 4, kString);
        }
        return d.ToArray();
    }

    private static void AddField(List<byte> d, string name, int number, int type)
    {
        var f = new List<byte>();
        PutStr(f, 1, name);
        PutInt(f, 3, (ulong)number);
        PutInt(f, 4, 1);
        PutInt(f, 5, (ulong)type);
        d.Add((2 << 3) | 2);
        PutVarint(d, (ulong)f.Count);
        d.AddRange(f);
    }

    private static byte[] ProtoRecord(int i)
    {
        var r = new List<byte>();
        PutStr(r, 1, "m-" + i);
        PutInt(r, 2, (ulong)i);
        PutInt(r, 3, 2);
        return r.ToArray();
    }

    private static void PutVarint(List<byte> o, ulong v)
    {
        while (v >= 0x80)
        {
            o.Add((byte)((v & 0x7f) | 0x80));
            v >>= 7;
        }
        o.Add((byte)v);
    }

    private static void PutStr(List<byte> o, int field, string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        o.Add((byte)((field << 3) | 2));
        PutVarint(o, (ulong)bytes.Length);
        o.AddRange(bytes);
    }

    private static void PutInt(List<byte> o, int field, ulong v)
    {
        o.Add((byte)((field << 3) | 0));
        PutVarint(o, v);
    }

    private static string Escape(string s)
    {
        var sb = new StringBuilder();
        foreach (var c in s)
        {
            if (c == '"' || c == '\\')
            {
                sb.Append('\\');
            }
            sb.Append(c == '\n' ? ' ' : c);
        }
        return sb.ToString();
    }
}
