using System.ComponentModel.DataAnnotations;

namespace Jellyfin.Database.Benchmarks.Entities;

/// <summary>
/// Test entity representing a user with concurrency token support.
/// </summary>
public class User : IHasConcurrencyToken
{
    /// <summary>
    /// Gets or sets the user ID.
    /// </summary>
    [Key]
    public int Id { get; set; }

    /// <summary>
    /// Gets or sets the username.
    /// </summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the email address.
    /// </summary>
    public string Email { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the last login timestamp.
    /// </summary>
    public DateTime LastLogin { get; set; }

    /// <inheritdoc />
    public uint ConcurrencyToken { get; set; }

    /// <inheritdoc />
    public void OnSavingChanges()
    {
        ConcurrencyToken++;
    }
}
