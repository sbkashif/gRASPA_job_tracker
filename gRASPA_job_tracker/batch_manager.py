import os
import math
import random
import shutil
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from tqdm import tqdm

class BatchManager:
    """Manage batching of CIF files for processing"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the batch manager
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.database_path = config['database']['path']
        self.batch_size = config['batch'].get('size', 100)
        self.strategy = config['batch'].get('strategy', 'alphabetical')
        self.size_thresholds = config['batch'].get('size_thresholds', [])
        
        # Use the batches directory from auto-generated config
        self.output_path = config['output']['output_dir']
        self.batch_dir = config['output']['batches_dir']
        
        # Find all CIF files in the database
        self.cif_files = self._find_cif_files()
        
        # Create a directory for batch information
        os.makedirs(self.batch_dir, exist_ok=True)
    
    def _find_cif_files(self) -> List[str]:
        """Find all CIF files in the database directory"""
        print(f"Searching for CIF files in: {self.database_path}")
        cif_files = []
        
        # Handle case where database_path is a file
        if os.path.isfile(self.database_path) and self.database_path.endswith('.cif'):
            return [self.database_path]
            
        if not os.path.exists(self.database_path):
            print(f"Warning: Database path {self.database_path} does not exist")
            return []
            
        # Search recursively for CIF files
        for root, _, files in os.walk(self.database_path):
            for file in files:
                if file.lower().endswith('.cif'):
                    cif_files.append(os.path.join(root, file))
    
        if cif_files:
            print(f"Found {len(cif_files)} CIF files")
        else:
            print("No CIF files found in the database directory")
    
        return cif_files
    
    def create_batches(self) -> List[List[str]]:
        """Create batches of CIF files using the specified strategy"""
        if not self.cif_files:
            print("No CIF files found. Cannot create batches.")
            return []
            
        print(f"Creating batches using strategy: {self.strategy}")
        
        if self.strategy == 'alphabetical':
            return self._create_alphabetical_batches()
        elif self.strategy == 'custom_alphabetical':
            return self._create_custom_alphabetical_batches()
        elif self.strategy == 'size_based':
            return self._create_size_based_batches()
        elif self.strategy == 'random':
            return self._create_random_batches()
        else:
            print(f"Warning: Unknown batching strategy '{self.strategy}'. Using alphabetical.")
            return self._create_alphabetical_batches()
    
    def _create_alphabetical_batches(self) -> List[List[str]]:
        """Create batches based on alphabetical ordering"""
        # Sort files alphabetically
        sorted_files = sorted(self.cif_files)
        
        return self._split_into_batches(sorted_files)
    
    def _create_custom_alphabetical_batches(self) -> List[List[str]]:
        """Create batches based on exact same alphabetical ordering as the bash script"""
        # Custom sorting function that ignores dots and ensures numbers come first
        def custom_sort_key(file_path):
            basename = os.path.basename(file_path).lower()
            
            # Remove dots completely for sorting purposes
            basename_no_dots = basename.replace('.', '')
            
            result = []
            for char in basename_no_dots:
                if char.isdigit():
                    # Make digits sort before letters
                    result.append(('0', char))
                else:
                    # Letters sort after digits
                    result.append(('1', char))
            
            return result
        
        # Sort files using the custom key function
        sorted_files = sorted(self.cif_files, key=custom_sort_key)
        
        # Save sorted file list to help with debugging (full paths)
        sorted_list_file = os.path.join(self.output_path, "sorted_files.txt")
        with open(sorted_list_file, 'w') as f:
            for file_path in sorted_files:
                f.write(f"{file_path}\n")
        
        # Save sorted basenames list for direct comparison with bash output
        sorted_basenames_file = os.path.join(self.output_path, "sorted_basenames.txt")
        with open(sorted_basenames_file, 'w') as f:
            for file_path in sorted_files:
                f.write(f"{os.path.basename(file_path)}\n")
        
        print(f"Saved sorted file lists to {sorted_list_file} and {sorted_basenames_file}")
        
        # Print sample of sorted files for verification
        print("Sample of sorted files (first 10):")
        for i, file_path in enumerate(sorted_files[:10]):
            print(f"  {i+1}: {os.path.basename(file_path)}")
        
        # Process all batches at once using the correct sorted files
        return self._split_into_batches(sorted_files)
    
    def _create_size_based_batches(self) -> List[List[str]]:
        """Create batches based on file sizes"""
        # If no size thresholds were provided, use alphabetical
        if not self.size_thresholds:
            print("No size thresholds provided for size_based strategy. Using alphabetical ordering.")
            return self._create_alphabetical_batches()
        
        # Group files by their size
        size_groups = {i: [] for i in range(len(self.size_thresholds) + 1)}
        
        for file_path in self.cif_files:
            try:
                file_size = os.path.getsize(file_path)
                
                # Determine which size group this file belongs to
                group_idx = 0
                for idx, threshold in enumerate(self.size_thresholds):
                    if file_size > threshold:
                        group_idx = idx + 1
                
                size_groups[group_idx].append(file_path)
            except OSError:
                print(f"Warning: Could not get size for {file_path}. Skipping.")
        
        # Create batches for each size group and combine
        all_batches = []
        for group_idx, files in size_groups.items():
            if files:
                print(f"Size group {group_idx}: {len(files)} files")
                group_batches = self._split_into_batches(sorted(files))
                all_batches.extend(group_batches)
        
        return all_batches
    
    def _create_random_batches(self) -> List[List[str]]:
        """Create batches with random file allocation"""
        # Make a copy and shuffle
        files_copy = self.cif_files.copy()
        random.shuffle(files_copy)
        
        return self._split_into_batches(files_copy)
    
    def _split_into_batches(self, files: List[str]) -> List[List[str]]:
        """Split a list of files into batches of specified size"""
        num_files = len(files)
        num_batches = math.ceil(num_files / self.batch_size)
        
        batches = []
        for i in range(num_batches):
            start_idx = i * self.batch_size
            end_idx = min((i + 1) * self.batch_size, num_files)
            batch = files[start_idx:end_idx]
            batches.append(batch)
            
            # Save batch to disk
            batch_num = len(batches)
            batch_df = pd.DataFrame({'file_path': batch})
            batch_file = os.path.join(self.batch_dir, f'batch_{batch_num}.csv')
            batch_df.to_csv(batch_file, index=False)
            
            # Create results directory for this batch
            batch_results_dir = os.path.join(self.config['output']['results_dir'], f'batch_{batch_num}')
            os.makedirs(batch_results_dir, exist_ok=True)
            
            print(f"Created batch {batch_num} with {len(batch)} files")
        
        print(f"Created {len(batches)} batches total")
        return batches
    
    def get_batch_files(self, batch_id: int) -> List[str]:
        """Get file paths for a specific batch"""
        # Ensure batch_id is an integer
        batch_id = int(batch_id)
        
        batch_file = os.path.join(self.batch_dir, f'batch_{batch_id}.csv')
        
        if not os.path.exists(batch_file):
            raise ValueError(f"Batch file not found: {batch_file}")
        
        try:
            batch_df = pd.read_csv(batch_file)
            # Ensure all file paths are strings and exist
            file_paths = []
            for file_path in batch_df['file_path'].tolist():
                file_path = str(file_path)
                if os.path.exists(file_path):
                    file_paths.append(file_path)
                else:
                    print(f"Warning: File does not exist: {file_path}")
            
            if not file_paths:
                print(f"Warning: No valid files in batch {batch_id}")
                
            return file_paths
        except Exception as e:
            print(f"Error reading batch file {batch_file}: {e}")
            return []
    
    def get_num_batches(self) -> int:
        """Get the total number of batches"""
        # If batch directory exists and has batch files, count actual batches
        if os.path.exists(self.batch_dir):
            batch_files = [f for f in os.listdir(self.batch_dir) 
                           if f.startswith('batch_') and f.endswith('.csv')]
            
            if batch_files:
                # Get the maximum batch number from filenames
                max_batch = 0
                for filename in batch_files:
                    try:
                        # Extract the number from batch_X.csv
                        batch_num = int(filename.replace('batch_', '').replace('.csv', ''))
                        max_batch = max(max_batch, batch_num)
                    except ValueError:
                        pass
                        
                return max_batch
        
        # If no batch files found or directory doesn't exist, 
        # calculate theoretical number (for initial batch creation)
        if not self.cif_files:
            return 0
        return math.ceil(len(self.cif_files) / self.batch_size)
    
    def has_batches(self) -> bool:
        """Check if any batches have been created"""
        if not os.path.exists(self.batch_dir):
            return False
            
        # Look for batch_*.csv files
        batch_files = [f for f in os.listdir(self.batch_dir) if f.startswith('batch_') and f.endswith('.csv')]
        return len(batch_files) > 0