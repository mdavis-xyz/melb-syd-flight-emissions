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

AUD_PER_POUND = 1.928246 # as of date of writing, 13/2/2026
KG_PER_TONNE = 1000
DAYS_PER_YEAR = 365

# =============================================================================
# MARGINAL DAMAGE COSTS ($/kg)
# =============================================================================
# These values represent the societal costs (health, climate) per kg of pollutant.

# AER guidance
# Table A.1, page 19
#https://www.aemc.gov.au/sites/default/files/2024-03/AEMC%20guide%20on%20how%20energy%20objectives%20shape%20our%20decisions%20clean%20200324.pdf
CO2_DAMAGE_PER_TONNE = 80 # AUD 2026 / tonne CO2e
CO2_DAMAGE_PER_KG = CO2_DAMAGE_PER_TONNE / KG_PER_TONNE

# IPCC AR4: CO indirect GWP100 = 1.9 (depletes OH → extends methane lifetime, enhances ozone)
# https://archive.ipcc.ch/publications_and_data/ar4/wg1/en/ch2s2-10-3-2.html
CO_GWP100 = 1.9 # kg CO2e per kg CO
CO_DAMAGE_PER_KG = CO2_DAMAGE_PER_KG * CO_GWP100

# https://www.gov.uk/government/publications/assess-the-impact-of-air-quality/air-quality-appraisal-damage-cost-guidance
# I haven't bothered indexing to inflation for 2026
# I'm using their SO2 figure. They don't have SOX.
SOX_DAMAGE_PER_TONNE_POUNDS = 26193
SOX_DAMAGE_PER_TONNE = SOX_DAMAGE_PER_TONNE_POUNDS * AUD_PER_POUND
SOX_DAMAGE_PER_KG = SOX_DAMAGE_PER_TONNE / KG_PER_TONNE


# https://www.gov.uk/government/publications/assess-the-impact-of-air-quality/air-quality-appraisal-damage-cost-guidance
# I haven't bothered indexing to inflation for 2026
NOX_DAMAGE_PER_TONNE_POUNDS = 10193
NOX_DAMAGE_PER_TONNE = NOX_DAMAGE_PER_TONNE_POUNDS * AUD_PER_POUND
NOX_DAMAGE_PER_KG = NOX_DAMAGE_PER_TONNE / KG_PER_TONNE


# https://www.gov.uk/government/publications/assess-the-impact-of-air-quality/air-quality-appraisal-damage-cost-guidance
# I haven't bothered indexing to inflation for 2026
# They say PM2.5. My understanding is that planes don't emit larger PM, so that's fine.
PM_DAMAGE_PER_TONNE_POUNDS = 111411
PM_DAMAGE_PER_TONNE = NOX_DAMAGE_PER_TONNE_POUNDS * AUD_PER_POUND
PM_DAMAGE_PER_KG = NOX_DAMAGE_PER_TONNE / KG_PER_TONNE

# https://www.gov.uk/government/publications/assess-the-impact-of-air-quality/air-quality-appraisal-damage-cost-guidance
# Local air quality damage from VOCs (ozone precursor, some carcinogenic)
HC_DAMAGE_PER_TONNE_POUNDS = 150
HC_DAMAGE_PER_TONNE_LOCAL = HC_DAMAGE_PER_TONNE_POUNDS * AUD_PER_POUND
HC_DAMAGE_PER_KG_LOCAL = HC_DAMAGE_PER_TONNE_LOCAL / KG_PER_TONNE

# Fry et al. 2014, ACP: anthropogenic NMVOC GWP20 for Australia
# (indirect, via ozone formation and extended methane lifetime)
# https://acp.copernicus.org/articles/14/523/2014/acp-14-523-2014.pdf table 4 AU GWP20
HC_GWP100 = 10.5 # kg CO2e per kg HC
HC_DAMAGE_PER_KG = HC_DAMAGE_PER_KG_LOCAL + HC_GWP100 * CO2_DAMAGE_PER_KG

# https://www.bitre.gov.au/sites/default/files/documents/domestic-aviation-activity-2024.pdf
# 2024, before Sydney's second airport openned
PASSENGERS_YEARLY = 8.04e6 
PASSENGERS_DAILY = PASSENGERS_YEARLY / DAYS_PER_YEAR

# =============================================================================

# Paths
DATA_DIR = Path("data")
RESULTS_DIR = DATA_DIR / "results"
PLANES_FILE = RESULTS_DIR / "planes.parquet"
EMISSIONS_FILE = RESULTS_DIR / "emissions.parquet"
PLANE_IMAGE = Path("plane-4.png")
PLANE_ANGLE = -45
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


def load_data(take_every):
    """Load planes and emissions data."""
    print("Loading data...")

    START_HOUR = 6
    planes_df = pl.read_parquet(PLANES_FILE).filter(pl.col("TIME").dt.hour() >= START_HOUR)
    emissions_df = pl.read_parquet(EMISSIONS_FILE).filter(pl.col("TIME").dt.hour() >= START_HOUR)

    # Sanity check: data should span exactly one local calendar day
    dates = emissions_df["TIME"].dt.date().unique()
    assert len(dates) == 1, f"Expected data for 1 day, got {len(dates)}: {sorted(dates.to_list())}"

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
    rotation_degrees = angle + PLANE_ANGLE

    # Rotate with transparent background (fillcolor with alpha=0)
    # Create a transparent image to use as fill color
    rotated = plane_img.rotate(
        rotation_degrees,
        expand=True,
        resample=Image.BICUBIC,
        fillcolor=(0, 0, 0, 0)  # Transparent black
    )

    return rotated


def create_frame(time_val, planes_at_time, emissions_at_time, flight_count_total, plane_img, frame_path):
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
    ax_map.set_aspect('equal')

    # Figure-level title centered across both subplots
    fig.suptitle('Melbourne – Sydney Flight Corridor\nDaily Emissions',
                 fontsize=20, fontweight='bold', y=0.95)

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

    num_passengers = round(PASSENGERS_DAILY * num_flights / flight_count_total)

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

    # Flight count
    y_pos -= 0.13
    ax_stats.text(0.5, y_pos, f'   Flight Count: {num_flights:>6.0f}',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)

    y_pos -= 0.06
    ax_stats.text(0.5, y_pos, f'Passenger Count: {num_passengers:>6.0f}',
                 ha='center', va='top', fontsize=18, family='monospace',
                 transform=ax_stats.transAxes)


    # Calculate dollar damages
    co2_damage = co2 * CO2_DAMAGE_PER_KG
    co_damage = co * CO_DAMAGE_PER_KG
    sox_damage = sox * SOX_DAMAGE_PER_KG
    nox_damage = nox * NOX_DAMAGE_PER_KG
    hc_damage = hc * HC_DAMAGE_PER_KG
    pm_damage = pm_total * PM_DAMAGE_PER_KG
    total_damage = co2_damage + co_damage + sox_damage + nox_damage + hc_damage + pm_damage

    # Table layout constants
    num_gases = 6
    num_rows = num_gases * 2 + 1  # 2 rows per gas (kg + $) + total row
    table_left = 0.15
    table_right = 0.85
    table_bottom = 0.08
    table_height = 0.52
    row_height = table_height / num_rows
    gas_col_width = 0.25
    value_col_x = table_left + gas_col_width

    # Build table data: gas names will be drawn separately for vertical centering
    table_data = [
        ['', f'{co2:>12,.0f} kg'],
        ['', f'{co2_damage:>12,.0f} $'],
        ['', f'{co:>12,.0f} kg'],
        ['', f'{co_damage:>12,.0f} $'],
        ['', f'{sox:>12,.0f} kg'],
        ['', f'{sox_damage:>12,.0f} $'],
        ['', f'{nox:>12,.0f} kg'],
        ['', f'{nox_damage:>12,.0f} $'],
        ['', f'{hc:>12,.0f} kg'],
        ['', f'{hc_damage:>12,.0f} $'],
        ['', f'{pm_total:>12,.0f} kg'],
        ['', f'{pm_damage:>12,.0f} $'],
        ['Total Damages', f'{total_damage:>12,.0f} $'],
    ]

    # Create table
    table = ax_stats.table(
        cellText=table_data,
        cellLoc='right',
        colWidths=[0.25, 0.45],
        loc='center',
        bbox=[table_left, table_bottom, 0.7, table_height]
    )

    # Style the table (booktabs style: no vertical lines, horizontal rules)
    table.auto_set_font_size(False)
    table.set_fontsize(14)

    dollar_rows = set(range(1, num_gases * 2, 2))  # 1, 3, 5, 7, 9, 11
    total_row = num_rows - 1

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('none')  # Remove all cell borders
        cell.set_text_props(family='monospace')
        cell.PAD = 0.02  # Reduce padding

        if col == 0:
            cell.set_text_props(ha='left', fontweight='bold')
            if row == total_row:
                cell.set_text_props(va='center')
        else:
            # Value column
            cell.set_text_props(ha='right')
            # Color the $ rows red
            if row in dollar_rows:
                cell.set_text_props(color='#C62828')
            elif row == total_row:
                cell.set_text_props(color='#B71C1C', fontweight='bold')

    # Draw gas names centered vertically between their two rows
    gas_names = [('CO₂', 0), ('CO', 2), ('SOₓ', 4), ('NOₓ', 6), ('HC', 8), ('PM', 10)]
    for gas_name, start_row in gas_names:
        # Calculate y position: center between start_row and start_row+1
        y_center = table_bottom + table_height - ((start_row + 1) * row_height)
        x_pos = table_left + 0.02
        ax_stats.text(x_pos, y_center, gas_name,
                     ha='left', va='center', fontsize=14, family='monospace',
                     fontweight='bold', transform=ax_stats.transAxes)

    # Draw horizontal separators between gas sections (booktabs style)
    separator_rows = list(range(2, num_gases * 2 + 1, 2))  # 2, 4, 6, 8, 10, 12
    for sep_row in separator_rows:
        y_line = table_bottom + table_height - (sep_row * row_height)
        ax_stats.plot([table_left, table_right], [y_line, y_line],
                     color='#888888', linewidth=1.0, transform=ax_stats.transAxes,
                     clip_on=False)

    # Top rule (thicker)
    y_top = table_bottom + table_height
    ax_stats.plot([table_left, table_right], [y_top, y_top],
                 color='black', linewidth=2.0, transform=ax_stats.transAxes,
                 clip_on=False)

    # Bottom rule (thicker)
    ax_stats.plot([table_left, table_right], [table_bottom, table_bottom],
                 color='black', linewidth=2.0, transform=ax_stats.transAxes,
                 clip_on=False)

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

    take_every = 4 # 20

    # Load data once
    planes_df, emissions_df, times = load_data(take_every=take_every)
    flight_count_total = emissions_df.select("NUM_FLIGHTS").max().item()
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
        create_frame(time_val, planes_at_time, emissions_at_time, flight_count_total, plane_img, frame_path)

    print(f"\nGenerated {len(times)} frames")

    # Create video
    video_duration = 20  # seconds
    framerate = len(times) // video_duration
    print(f"{framerate=}")
    create_video(FRAME_DIR, OUTPUT_VIDEO, framerate=framerate)

    print(f"\nDone! Video saved to: {OUTPUT_VIDEO}")


if __name__ == "__main__":
    main()
