import pandas as pd
import numpy as np
from datetime import datetime
import psutil
import os
import time
import json

class PandasETL:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None
        self.metrics = {}
    
    def measure_memory(self):
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024  # MB
    
    def load_data(self):
        """Load CSV data with timing"""
        print("Loading data...")
        start_time = time.time()
        start_memory = self.measure_memory()
        
        self.df = pd.read_csv(self.file_path)
        
        end_time = time.time()
        end_memory = self.measure_memory()
        
        self.metrics['load_time'] = end_time - start_time
        self.metrics['load_memory'] = end_memory - start_memory
        self.metrics['rows_loaded'] = len(self.df)
        
        print(f"✅ Loaded {len(self.df):,} rows in {self.metrics['load_time']:.2f}s")
        print(f"Memory used: {self.metrics['load_memory']:.1f} MB")
        return self
    
    def clean_data(self):
        """Clean and validate data"""
        print("Cleaning data...")
        start_time = time.time()
        initial_rows = len(self.df)
        
        # Remove rows with invalid coordinates
        self.df = self.df[
            (self.df['pickup_longitude'] != 0) & 
            (self.df['pickup_latitude'] != 0) &
            (self.df['dropoff_longitude'] != 0) & 
            (self.df['dropoff_latitude'] != 0)
        ]
        
        # Remove invalid trip distances and passenger counts
        self.df = self.df[
            (self.df['trip_distance'] > 0) & 
            (self.df['trip_distance'] < 100) &
            (self.df['passenger_count'] > 0) & 
            (self.df['passenger_count'] <= 6)
        ]
        
        # Convert datetime columns
        self.df['tpep_pickup_datetime'] = pd.to_datetime(self.df['tpep_pickup_datetime'])
        self.df['tpep_dropoff_datetime'] = pd.to_datetime(self.df['tpep_dropoff_datetime'])
        
        # Calculate trip duration in minutes
        self.df['trip_duration_minutes'] = (
            self.df['tpep_dropoff_datetime'] - self.df['tpep_pickup_datetime']
        ).dt.total_seconds() / 60
        
        # Remove invalid durations (negative or too long)
        self.df = self.df[
            (self.df['trip_duration_minutes'] > 0) & 
            (self.df['trip_duration_minutes'] < 480)  # 8 hours max
        ]
        
        end_time = time.time()
        self.metrics['clean_time'] = end_time - start_time
        self.metrics['rows_after_cleaning'] = len(self.df)
        rows_removed = initial_rows - len(self.df)
        
        print(f"✅ Cleaned data in {self.metrics['clean_time']:.2f}s")
        print(f"Removed {rows_removed:,} invalid rows, {len(self.df):,} rows remaining")
        return self
    
    def aggregate_data(self):
        """Perform complex aggregations"""
        print("Performing aggregations...")
        start_time = time.time()
        
        # Add date and hour columns for grouping
        self.df['date'] = self.df['tpep_pickup_datetime'].dt.date
        self.df['hour'] = self.df['tpep_pickup_datetime'].dt.hour
        self.df['day_of_week'] = self.df['tpep_pickup_datetime'].dt.day_name()
        
        # Daily trip statistics
        daily_stats = self.df.groupby('date').agg({
            'trip_distance': ['count', 'mean', 'sum', 'std'],
            'trip_duration_minutes': ['mean', 'sum'],
            'passenger_count': ['sum', 'mean'],
            'total_amount': ['mean', 'sum', 'std']
        }).reset_index()
        
        # Hourly patterns
        hourly_stats = self.df.groupby('hour').agg({
            'trip_distance': ['count', 'mean'],
            'trip_duration_minutes': 'mean',
            'total_amount': 'mean',
            'passenger_count': 'mean'
        }).reset_index()
        
        # Day of week patterns
        dow_stats = self.df.groupby('day_of_week').agg({
            'trip_distance': ['count', 'mean'],
            'total_amount': 'mean'
        }).reset_index()
        
        # Passenger count distribution
        passenger_dist = self.df['passenger_count'].value_counts().sort_index()
        
        # Distance bins analysis
        self.df['distance_bin'] = pd.cut(
            self.df['trip_distance'], 
            bins=[0, 1, 3, 5, 10, 100], 
            labels=['Short (0-1mi)', 'Medium (1-3mi)', 'Long (3-5mi)', 'Very Long (5-10mi)', 'Extreme (10+mi)']
        )
        distance_analysis = self.df.groupby('distance_bin').agg({
            'trip_distance': 'count',
            'total_amount': 'mean',
            'trip_duration_minutes': 'mean'
        }).reset_index()
        
        end_time = time.time()
        self.metrics['aggregate_time'] = end_time - start_time
        
        print(f"✅ Aggregated data in {self.metrics['aggregate_time']:.2f}s")
        
        # Store results
        self.daily_stats = daily_stats
        self.hourly_stats = hourly_stats
        self.dow_stats = dow_stats
        self.passenger_dist = passenger_dist
        self.distance_analysis = distance_analysis
        
        return self
    
    def sort_and_filter(self):
        """Perform sorting and filtering operations"""
        print("Sorting and filtering...")
        start_time = time.time()
        
        # Sort by trip distance (descending) - expensive operation
        sorted_by_distance = self.df.sort_values('trip_distance', ascending=False)
        
        # Multiple filters
        long_trips = self.df[self.df['trip_distance'] > 10]
        expensive_trips = self.df[self.df['total_amount'] > 50]
        rush_hour_trips = self.df[self.df['hour'].isin([7, 8, 9, 17, 18, 19])]
        weekend_trips = self.df[self.df['day_of_week'].isin(['Saturday', 'Sunday'])]
        
        # Complex filtering
        premium_trips = self.df[
            (self.df['trip_distance'] > 5) & 
            (self.df['total_amount'] > 30) & 
            (self.df['passenger_count'] >= 2)
        ]
        
        end_time = time.time()
        self.metrics['sort_filter_time'] = end_time - start_time
        self.metrics['long_trips_count'] = len(long_trips)
        self.metrics['expensive_trips_count'] = len(expensive_trips)
        self.metrics['rush_hour_trips_count'] = len(rush_hour_trips)
        self.metrics['weekend_trips_count'] = len(weekend_trips)
        self.metrics['premium_trips_count'] = len(premium_trips)
        
        print(f"✅ Sorted and filtered data in {self.metrics['sort_filter_time']:.2f}s")
        print(f"Found {len(long_trips):,} long trips, {len(expensive_trips):,} expensive trips")
        return self
    
    def save_results(self, output_dir):
        """Save processed results to files"""
        print("Saving results...")
        start_time = time.time()
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Save aggregated results as CSV
        self.daily_stats.to_csv(f"{output_dir}/pandas_daily_stats.csv", index=False)
        self.hourly_stats.to_csv(f"{output_dir}/pandas_hourly_stats.csv", index=False)
        self.dow_stats.to_csv(f"{output_dir}/pandas_dow_stats.csv", index=False)
        self.distance_analysis.to_csv(f"{output_dir}/pandas_distance_analysis.csv", index=False)
        
        # Save summary statistics
        summary = {
            'total_rows': len(self.df),
            'total_distance': float(self.df['trip_distance'].sum()),
            'avg_trip_distance': float(self.df['trip_distance'].mean()),
            'total_revenue': float(self.df['total_amount'].sum()),
            'avg_fare': float(self.df['total_amount'].mean()),
            'date_range': {
                'start': str(self.df['tpep_pickup_datetime'].min()),
                'end': str(self.df['tpep_pickup_datetime'].max())
            }
        }
        
        with open(f"{output_dir}/pandas_summary.json", 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Save performance metrics
        with open(f"{output_dir}/pandas_metrics.json", 'w') as f:
            json.dump(self.metrics, f, indent=2, default=str)
        
        end_time = time.time()
        self.metrics['save_time'] = end_time - start_time
        
        print(f"✅ Results saved in {self.metrics['save_time']:.2f}s")
        return self

def run_pandas_benchmark(file_path, output_dir):
    """Run complete Pandas benchmark"""
    print("=" * 50)
    print("🐼 STARTING PANDAS ETL BENCHMARK")
    print("=" * 50)
    
    total_start = time.time()
    peak_memory = 0
    
    try:
        etl = PandasETL(file_path)
        
        # Run all operations
        etl.load_data().clean_data().aggregate_data().sort_and_filter().save_results(output_dir)
        
        # Calculate total time and peak memory
        total_time = time.time() - total_start
        etl.metrics['total_time'] = total_time
        peak_memory = max([v for k, v in etl.metrics.items() if isinstance(v, (int, float)) and 'memory' in k])
        etl.metrics['peak_memory'] = peak_memory
        
        # Final summary
        print("\n" + "=" * 50)
        print("🎉 PANDAS BENCHMARK COMPLETE!")
        print("=" * 50)
        print(f"⏱️  Total time: {total_time:.2f} seconds")
        print(f"🧠 Peak memory: {peak_memory:.1f} MB")
        print(f"📊 Processed: {etl.metrics['rows_loaded']:,} rows")
        print(f"✨ Clean data: {etl.metrics['rows_after_cleaning']:,} rows")
        print("=" * 50)
        
        return etl.metrics
        
    except Exception as e:
        print(f"❌ Error during Pandas benchmark: {e}")
        return None

if __name__ == "__main__":
    # Check if data file exists
    data_file = "../data/yellow_tripdata_2015-01.csv"
    if not os.path.exists(data_file):
        print(f"❌ Data file not found: {data_file}")
        print("Please ensure the NYC taxi dataset is in the data/ directory")
        exit(1)
    
    # Run benchmark
    metrics = run_pandas_benchmark(data_file, "../results")
    
    if metrics:
        print("\n📈 Key Performance Metrics:")
        for key, value in metrics.items():
            if 'time' in key and isinstance(value, (int, float)):
                print(f"  {key.replace('_', ' ').title()}: {value:.2f}s")

