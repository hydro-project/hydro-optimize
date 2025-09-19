#!/usr/bin/env python3
"""
Graph Anomalies Visualization Tool

This script parses performance data from Hydroflow optimization output and creates
box plots to visualize CPU usage and cardinality metrics, grouped by operator location.
Outliers (outside the interquartile range) are labeled with their operator IDs.
"""

import re
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import argparse
import os
import glob


def parse_cluster_names(filepath):
    """
    Parse cluster name mappings from lines like:
    "Analyzing cluster Cluster(0): hydro_test::cluster::simple_graphs::Server"
    
    Returns:
        dict: Mapping from cluster ID (e.g., "Cluster(0)") to friendly name (e.g., "Server")
    """
    cluster_names = {}
    pattern = r'Analyzing cluster (Cluster\(\d+\)): (.+)$'
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            match = re.match(pattern, line)
            if match:
                cluster_id = match.group(1)
                full_name = match.group(2).strip()
                
                # Extract the simple name from the full path
                if '::' in full_name:
                    simple_name = full_name.split('::')[-1]
                elif full_name == '()':
                    simple_name = 'New cluster'  # Keep original name if empty
                else:
                    simple_name = full_name
                
                cluster_names[cluster_id] = simple_name
    
    return cluster_names


def parse_new_location_lines(filepath):
    """
    Parse lines starting with 'New location' from the input file.
    
    Returns:
        dict: Dictionary with metric type as key, containing location data
    """
    data_by_metric = defaultdict(lambda: defaultdict(list))
    decoupled_data = defaultdict(lambda: defaultdict(dict))  # Store decoupled send/recv separately first
    
    # General pattern to match the new format
    # New location <location>, old location <old_location>: Operator <id> <metric_type>, new value <new_value>, old value <old_value>
    pattern = r'New location ((?:Cluster|Process)\(\d+\)), old location (?:Cluster|Process)\(\d+\): Operator (\d+) ([^,]+), new value (.*?), old value (.*?)$'
    
    def parse_value(value_str):
        """Parse values like 'None' or 'Some(123.45)' """
        value_str = value_str.strip()
        if value_str == 'None':
            return None
        elif value_str.startswith('Some(') and value_str.endswith(')'):
            try:
                return float(value_str[5:-1])  # Extract value from Some(value)
            except ValueError:
                return None
        else:
            try:
                return float(value_str)
            except ValueError:
                return None
    
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line.startswith('New location'):
                continue
            
            match = re.match(pattern, line)
            if match:
                location = match.group(1)
                operator_id = match.group(2)
                metric_type = match.group(3).strip()
                new_value = parse_value(match.group(4))
                old_value = parse_value(match.group(5))
                
                # Handle special cases that contain the string 'CPU usage'
                if metric_type != 'CPU usage' and 'CPU usage' in metric_type:
                    metric_type_without_CPU_usage = metric_type.replace('CPU usage', '').strip()
                    operator_id = f'{operator_id}_{metric_type_without_CPU_usage}'
                    metric_type = 'CPU usage'
                
                # Only include data points where both values are available
                if new_value is not None and old_value is not None and old_value > 0:
                    scale_factor = new_value / old_value
                    
                    data_by_metric[metric_type][location].append({
                        'operator_id': operator_id,
                        'scale_factor': scale_factor,
                        'old_value': old_value,
                        'new_value': new_value,
                        'line_num': line_num
                    })
                elif new_value is not None and old_value is None:
                    # New metric with no previous value
                    data_by_metric[metric_type][location].append({
                        'operator_id': operator_id,
                        'scale_factor': float('inf'),  # Special case for new metrics
                        'old_value': 0.0,
                        'new_value': new_value,
                        'line_num': line_num
                    })
            else:
                print(f"Warning: Could not parse line {line_num}: {line}")
    return data_by_metric


def create_box_plot(data, title, ylabel, filename, cluster_names=None):
    """
    Create a box plot with outlier labels.
    
    Args:
        data: Dictionary with location as key and list of data points as value
        title: Plot title
        ylabel: Y-axis label
        filename: Output filename
        cluster_names: Dictionary mapping cluster IDs to friendly names
    """
    if not data:
        print(f"No data available for {title}")
        return
    
    # Prepare data for plotting
    locations = list(data.keys())
    values_by_location = []
    all_values = []
    all_operator_ids = []
    location_labels = []
    
    # Map cluster IDs to friendly names for display
    display_locations = []
    for location in locations[:]:
        if cluster_names and location in cluster_names:
            display_name = cluster_names[location]
            if display_name == 'Client':
                locations.remove(location)
                continue  # Skip metrics for client
            display_locations.append(cluster_names[location])
        else:
            display_locations.append(location)
    
    for location in locations:
        location_values = []
        location_operator_ids = []
        
        for point in data[location]:
            # Use scale factor as the metric
            if point['scale_factor'] == float('inf'):
                # For new CPU usage, use a large scale factor
                value = 100.0  # Arbitrary large value to represent "new"
            else:
                value = point['scale_factor']
            
            location_values.append(value)
            location_operator_ids.append(point['operator_id'])
        
        values_by_location.append(location_values)
        all_values.extend(location_values)
        all_operator_ids.extend(location_operator_ids)
        location_labels.extend([location] * len(location_values))
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create box plot with display names
    bp = ax.boxplot(values_by_location, tick_labels=display_locations, patch_artist=True)
    
    # Color the boxes
    colors = ['lightblue', 'lightgreen', 'lightcoral', 'lightyellow', 'lightpink']
    for patch, color in zip(bp['boxes'], colors[:len(bp['boxes'])]):
        patch.set_facecolor(color)
    
    # Add scatter plot overlay for individual operator values
    scatter_colors = ['darkblue', 'darkgreen', 'darkred', 'orange', 'purple']
    for i, (location, values) in enumerate(zip(locations, values_by_location)):
        # Add some random jitter to x-coordinates for better visibility
        x_positions = np.random.normal(i + 1, 0.04, size=len(values))
        scatter_color = scatter_colors[i % len(scatter_colors)]
        
        ax.scatter(x_positions, values, 
                  alpha=0.6, 
                  s=30, 
                  color=scatter_color,
                  edgecolors='black',
                  linewidth=0.5,
                  zorder=3,  # Ensure scatter points appear in front of box plot
                  label=f'{display_locations[i]} operators' if i < len(display_locations) else f'{location} operators')
    
    # Calculate quartiles for outlier detection and labeling
    # for i, (location, values) in enumerate(zip(locations, values_by_location)):
    #     if len(values) < 4:  # Need at least 4 points for quartiles
    #         continue
            
    #     q1 = np.percentile(values, 25)
    #     q3 = np.percentile(values, 75)
    #     iqr = q3 - q1
    #     lower_bound = q1 - 1.5 * iqr
    #     upper_bound = q3 + 1.5 * iqr
        
        # Find outliers and label them
        for j, (value, operator_id) in enumerate(zip(values, [point['operator_id'] for point in data[location]])):
            # if value < lower_bound or value > upper_bound:
            # Add label for outlier
            ax.annotate(f'Op {operator_id}', 
                        xy=(i + 1, value), 
                        xytext=(5, 5), 
                        textcoords='offset points',
                        fontsize=8,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.set_xlabel('New Location', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, alpha=0.3)
    
    # Add legend for scatter plot
    ax.legend(loc='upper right', fontsize=10, framealpha=0.9)
    
    # Rotate x-axis labels if needed
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Box plot saved as {filename}")
    
    # Show statistics
    print(f"\n{title} Statistics:")
    for i, location in enumerate(locations):
        display_name = display_locations[i]
        values = [point['scale_factor'] if point['scale_factor'] != float('inf') else 100.0 
                 for point in data[location]]
        if values:
            print(f"  {display_name}: {len(values)} operators, "
                  f"mean scale factor: {np.mean(values):.3f}, "
                  f"median: {np.median(values):.3f}")


def main():
    parser = argparse.ArgumentParser(description='Create box plots for graph anomalies analysis')
    parser.add_argument('input_directory', nargs='?', default='.',
                       help='Directory containing .txt files with "New location" lines (default: current directory)')
    args = parser.parse_args()
    
    try:
        # Find all .txt files in the directory
        txt_pattern = os.path.join(args.input_directory, '*.txt')
        txt_files = glob.glob(txt_pattern)
        
        if not txt_files:
            print(f"No .txt files found in directory: {args.input_directory}")
            return
        
        print(f"Found {len(txt_files)} .txt files in {args.input_directory}")
        for txt_file in txt_files:
            print(f"  - {os.path.basename(txt_file)}")
        
        # Process each file individually
        for txt_file in txt_files:
            print(f"\nProcessing {os.path.basename(txt_file)}...")
            
            # Parse cluster names from this file
            cluster_names = parse_cluster_names(txt_file)
            print(f"  Found cluster mappings: {cluster_names}")
            
            # Parse the data from this file
            data_by_metric = parse_new_location_lines(txt_file)
            print(f"  Found metrics: {list(data_by_metric.keys())}")
            
            # Get the base filename without extension for output files
            base_filename = os.path.splitext(txt_file)[0]
            
            # Create box plot for each metric type from this file
            for metric_type, location_data in data_by_metric.items():
                print(f"  Creating box plot for {metric_type}...")
                total_operators = sum(len(data_points) for data_points in location_data.values())
                print(f"    Found {total_operators} operators across {len(location_data)} locations")
                
                # Create safe filename from metric type
                safe_filename = metric_type.lower().replace(' ', '_').replace('/', '_')
                output_filename = f"{base_filename}_{safe_filename}.png"
                
                create_box_plot(
                    location_data,
                    f'{metric_type.title()} Scale Factors by New Location - {os.path.basename(txt_file)}',
                    f'{metric_type.title()} Scale Factor',
                    output_filename,
                    cluster_names
                )
        
        # Show plots
        plt.show()
        
    except FileNotFoundError:
        print(f"Error: Directory '{args.input_directory}' not found.")
        print("Please make sure the directory exists and try again.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()