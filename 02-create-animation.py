#!/usr/bin/env python
"""
Create animation of Melbourne-Sydney flights with emissions data.
"""

import os
import subprocess
import tempfile
from pathlib import Path
from shutil import rmtree

from tqdm import tqdm
import polars as pl
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for parallel processing
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from PIL import Image
import numpy as np
import contextily as cx
from pyproj import Transformer

# Paths
DATA_DIR = Path("data")
RESULTS_DIR = DATA_DIR / "results"
PLANES_FILE = RESULTS_DIR / "planes.parquet"
EMISSIONS_FILE = RESULTS_DIR / "emissions.parquet"
PLANE_IMAGE = Path("plane.png")
OUTPUT_VIDEO = RESULTS_DIR / "animation.mp4"
FRAME_DIR = RESULTS_DIR / "frames"

# Airport coordinates (lat/lon)
SYDNEY = {"lat": -33.946111, "lon": 151.177222}
MELBOURNE = {"lat": -37.673333, "lon": 144.843333}

# Map bounds (with padding) in lat/lon
LAT_MIN, LAT_MAX = -38.5, -33.0
LON_MIN, LON_MAX = 144.0, 152.0

# Coordinate transformer: lat/lon (EPSG:4326) to Web Mercator (EPSG:3857)
transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

# Convert bounds to Web Mercator
X_MIN, Y_MIN = transformer.transform(LON_MIN, LAT_MIN)
X_MAX, Y_MAX = transformer.transform(LON_MAX, LAT_MAX)

# Convert airport coords to Web Mercator
SYDNEY_X, SYDNEY_Y = transformer.transform(SYDNEY['lon'], SYDNEY['lat'])
MELBOURNE_X, MELBOURNE_Y = transformer.transform(MELBOURNE['lon'], MELBOURNE['lat'])


def load_data(take_every=6):
    """Load planes and emissions data."""
    print("Loading data...")

    START_HOUR = 6
    planes_df = pl.read_parquet(PLANES_FILE).filter(pl.col("TIME").dt.hour() > START_HOUR)
    emissions_df = pl.read_parquet(EMISSIONS_FILE).filter(pl.col("TIME").dt.hour() > START_HOUR)

    # take 1 in every nth record, to speed up generation
    emissions_df = emissions_df.gather_every(take_every)

    # Filter planes to only include times that are in emissions
    planes_df = planes_df.join(emissions_df.select("TIME"), how='inner', on="TIME")

    # Get unique timestamps
    times = sorted(emissions_df["TIME"].unique().to_list())

    print(f"Found {len(times)} timestamps to process")
    print(f"Planes data shape: {planes_df.shape}")
    print(f"Emissions data shape: {emissions_df.shape}")

    return planes_df, emissions_df, times


def load_and_prepare_plane_image():
    """Load the plane image with transparency."""
    # print(f"Loading plane image from {PLANE_IMAGE}")
    plane_img = Image.open(PLANE_IMAGE)

    # Convert to RGBA to ensure transparency is handled properly
    if plane_img.mode != 'RGBA':
        plane_img = plane_img.convert('RGBA')

    # print(f"Plane image size: {plane_img.size}, mode: {plane_img.mode}")

    return plane_img


def rotate_plane_image(plane_img, angle):
    """
    Rotate plane image to correct orientation while preserving transparency.

    plane.png points north (90°).
    ANGLE column: 0=east, 90=north.
    Rotation needed: (90 - angle) degrees clockwise = (angle - 90) degrees counterclockwise.
    """
    rotation_degrees = angle - 90

    # Rotate with transparent background (fillcolor with alpha=0)
    # Create a transparent image to use as fill color
    rotated = plane_img.rotate(
        rotation_degrees,
        expand=True,
        resample=Image.BICUBIC,
        fillcolor=(0, 0, 0, 0)  # Transparent black
    )

    return rotated


def create_frame(time_val, planes_at_time, emissions_at_time, plane_img, frame_path):
    """Create a single frame of the animation."""

    # Create figure with two subplots
    # Use dimensions that result in even pixel counts (divisible by 2 for h264)
    fig = plt.figure(figsize=(16, 9), dpi=120)

    # Left subplot: Map
    ax_map = fig.add_subplot(1, 2, 1)
    ax_map.set_xlim(X_MIN, X_MAX)
    ax_map.set_ylim(Y_MIN, Y_MAX)
    ax_map.set_xlabel('', fontsize=12)
    ax_map.set_ylabel('', fontsize=12)
    ax_map.set_title('Melbourne - Sydney Flight Corridor', fontsize=14, fontweight='bold')
    ax_map.set_aspect('equal')

    # Add basemap tiles (OpenStreetMap style)
    cx.add_basemap(ax_map, crs="EPSG:3857", source=cx.providers.OpenStreetMap.Mapnik, zoom=7)

    # Hide tick labels (we have the map now)
    ax_map.set_xticks([])
    ax_map.set_yticks([])

    # Mark airports
    ax_map.plot(SYDNEY_X, SYDNEY_Y, 'ro', markersize=12, label='Sydney', zorder=5,
                markeredgecolor='white', markeredgewidth=2)
    ax_map.plot(MELBOURNE_X, MELBOURNE_Y, 'bo', markersize=12, label='Melbourne', zorder=5,
                markeredgecolor='white', markeredgewidth=2)
    ax_map.legend(loc='upper left', fontsize=11, framealpha=0.9)

    # Add planes
    if len(planes_at_time) > 0:
        for row in planes_at_time.iter_rows(named=True):
            lat = row['LATITUDE']
            lon = row['LONGITUDE']
            angle = row['ANGLE']
            in_air = row['IN_AIR']

            if in_air:
                # Convert lat/lon to Web Mercator
                x, y = transformer.transform(lon, lat)

                # Rotate plane image
                rotated_plane = rotate_plane_image(plane_img, angle)

                # Create OffsetImage (adjust zoom for appropriate size)
                imagebox = OffsetImage(rotated_plane, zoom=0.03)
                ab = AnnotationBbox(imagebox, (x, y), frameon=False, pad=0, zorder=10)
                ax_map.add_artist(ab)

    # Right subplot: Statistics
    ax_stats = fig.add_subplot(1, 2, 2)
    ax_stats.axis('off')

    # Get emissions data for this time
    if len(emissions_at_time) > 0:
        num_flights = emissions_at_time['NUM_FLIGHTS'][0]
        co2 = emissions_at_time['CO2'][0]
        nox = emissions_at_time['NOX'][0]
        sox = emissions_at_time['SOX'][0]
        co = emissions_at_time['CO'][0]
        hc = emissions_at_time['HC'][0]
        pm_total = emissions_at_time['PM_TOTAL'][0]
    else:
        num_flights = co2 = nox = sox = co = hc = pm_total = 0.0

    # Format time
    time_str = time_val.strftime('%H:%M')

    # Draw analog clock
    clock_center_x = 0.25
    clock_center_y = 0.82
    clock_radius = 0.08

    # Clock face (circle)
    clock_face = plt.Circle((clock_center_x, clock_center_y), clock_radius,
                             transform=ax_stats.transAxes, facecolor='white',
                             edgecolor='black', linewidth=2, zorder=1)
    ax_stats.add_patch(clock_face)

    # Calculate hand angles (0 degrees = 12 o'clock, clockwise)
    hours = time_val.hour % 12
    minutes = time_val.minute

    # Minute hand: 6 degrees per minute, measured from 12 o'clock
    minute_angle = np.radians(90 - minutes * 6)
    minute_length = clock_radius * 0.85
    minute_x = clock_center_x + minute_length * np.cos(minute_angle)
    minute_y = clock_center_y + minute_length * np.sin(minute_angle)

    # Hour hand: 30 degrees per hour + 0.5 degrees per minute
    hour_angle = np.radians(90 - (hours * 30 + minutes * 0.5))
    hour_length = clock_radius * 0.55
    hour_x = clock_center_x + hour_length * np.cos(hour_angle)
    hour_y = clock_center_y + hour_length * np.sin(hour_angle)

    # Draw hands
    ax_stats.plot([clock_center_x, minute_x], [clock_center_y, minute_y],
                  color='black', linewidth=2, transform=ax_stats.transAxes, zorder=2)
    ax_stats.plot([clock_center_x, hour_x], [clock_center_y, hour_y],
                  color='black', linewidth=3, transform=ax_stats.transAxes, zorder=2)

    # Center dot
    center_dot = plt.Circle((clock_center_x, clock_center_y), 0.008,
                             transform=ax_stats.transAxes, facecolor='black', zorder=3)
    ax_stats.add_patch(center_dot)

    # Display time text (moved right)
    y_pos = 0.85
    ax_stats.text(0.6, y_pos, time_str,
                 ha='center', va='top', fontsize=48, fontweight='bold',
                 family='monospace', transform=ax_stats.transAxes)

    y_pos -= 0.15
    ax_stats.text(0.5, y_pos, 'Cumulative Emissions',
                 ha='center', va='top', fontsize=24, fontweight='bold',
                 transform=ax_stats.transAxes)

    # Flight count
    y_pos -= 0.10
    ax_stats.text(0.5, y_pos, f'Flight Count: {num_flights:>6.0f}',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    # Emissions counters (odometer style with zero padding)
    y_pos -= 0.07
    ax_stats.text(0.5, y_pos, f'CO₂:  {co2:012,.0f} kg',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    y_pos -= 0.07
    ax_stats.text(0.5, y_pos, f'NOₓ:  {nox:012,.0f} kg',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    y_pos -= 0.07
    ax_stats.text(0.5, y_pos, f'SOₓ:  {sox:012,.0f} kg',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    y_pos -= 0.07
    ax_stats.text(0.5, y_pos, f'CO:   {co:012,.0f} kg',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    y_pos -= 0.07
    ax_stats.text(0.5, y_pos, f'HC:   {hc:012,.0f} kg',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    y_pos -= 0.07
    ax_stats.text(0.5, y_pos, f'PM:   {pm_total:012,.0f} kg',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    # Save frame
    plt.tight_layout()
    # Don't use bbox_inches='tight' to maintain exact dimensions (16*120=1920, 9*120=1080)
    plt.savefig(frame_path, dpi=120)
    plt.close(fig)


def create_video(frame_dir, output_path, framerate=10):
    """Use ffmpeg to create video from frames."""
    print(f"\nCreating video with ffmpeg...")

    # ffmpeg command
    cmd = [
        'ffmpeg',
        '-y',  # Overwrite output file
        '-framerate', str(framerate),
        '-pattern_type', 'glob',
        '-i', f'{frame_dir}/frame_*.png',
        '-c:v', 'libx264',
        '-pix_fmt', 'yuv420p',
        '-crf', '23',  # Quality (lower = better, 23 is default)
        str(output_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("FFMPEG Error:")
        print(result.stderr)
        raise RuntimeError("Failed to create video")

    print(f"Video created: {output_path}")
    print(f"Video size: {output_path.stat().st_size / (1024*1024):.2f} MB")


def main():
    """Main function to create the animation."""
    print("Starting animation creation...")

    take_every = 2

    # Load data once
    planes_df, emissions_df, times = load_data(take_every=take_every)
    plane_img = load_and_prepare_plane_image()

    if os.path.exists(FRAME_DIR):
        rmtree(FRAME_DIR)
    os.makedirs(FRAME_DIR)

    # Generate frames sequentially
    for i, time_val in enumerate(tqdm(times, desc="Generating frames")):
        # Filter data for this timestamp
        planes_at_time = planes_df.filter(pl.col("TIME") == time_val)
        emissions_at_time = emissions_df.filter(pl.col("TIME") == time_val)

        # Create frame
        frame_path = FRAME_DIR / f"frame_{i:04d}.png"
        create_frame(time_val, planes_at_time, emissions_at_time, plane_img, frame_path)

    print(f"\nGenerated {len(times)} frames")

    # Create video
    video_duration = 20  # seconds
    framerate = len(times) // video_duration
    print(f"{framerate=}")
    create_video(FRAME_DIR, OUTPUT_VIDEO, framerate=framerate)

    print(f"\nDone! Video saved to: {OUTPUT_VIDEO}")


if __name__ == "__main__":
    main()
