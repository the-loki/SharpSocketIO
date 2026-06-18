namespace SharpSocketIO.EngineIo.Commons;

public sealed class CookieOptions
{
    public int? MaxAge { get; set; }
    public System.DateTime? Expires { get; set; }
    public string? Path { get; set; }
    public string? Domain { get; set; }
    public bool? Secure { get; set; }
    public bool? HttpOnly { get; set; }
    /// <summary>SameSite mode: "lax" / "strict" / "none" (JS semantics).</summary>
    public string? SameSite { get; set; }
    public bool? Signed { get; set; }
    public bool? Overwrite { get; set; }
}
