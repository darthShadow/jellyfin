using System.ComponentModel.DataAnnotations;

namespace Jellyfin.Database.Benchmarks.Entities;

/// <summary>
/// Test entity representing a movie with concurrency token support.
/// </summary>
public class Movie : IHasConcurrencyToken
{
    /// <summary>
    /// Gets or sets the movie ID.
    /// </summary>
    [Key]
    public int Id { get; set; }

    /// <summary>
    /// Gets or sets the movie title.
    /// </summary>
    public string Title { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the year released.
    /// </summary>
    public int Year { get; set; }

    /// <summary>
    /// Gets or sets the rating.
    /// </summary>
    public double Rating { get; set; }

    /// <summary>
    /// Gets or sets the view count.
    /// </summary>
    public int ViewCount { get; set; }

    /// <inheritdoc />
    public uint ConcurrencyToken { get; set; }

    /// <inheritdoc />
    public void OnSavingChanges()
    {
        ConcurrencyToken++;
    }
}
