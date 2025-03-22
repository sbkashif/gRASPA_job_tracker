import os
import math
from typing import List, Dict, Any
import pandas as pd

class BatchManager:
    """Manage batching of CIF files for processing"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the batch manager
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.database_path = config['database_path']
        self.batch_size = config['batch_size']
        self.output_path = config['output_path']
        
        # Find all CIF files in the database
        self.cif_files = self._find_cif_files()
        
        # Create a directory for batch information
        self.batch_dir = os.path.join(self.output_path, 'batches')
        os.makedirs(self.batch_dir, exist_ok=True)
    
    def _find_cif_files(self) -> List[str]:
        """Find all CIF files in the database directory"""
        cif_files = []
        
        for root, _, files in os.walk(self.database_path):
            for file in files:
                if file.endswith('.cif'):
                    cif_files.append(os.path.join(root, file))
        
        return cif_files
    
    def create_batches(self) -> List[List[str]]:
        """Create batches of CIF files"""
        num_files = len(self.cif_files)
        num_batches = math.ceil(num_files / self.batch_size)
        
        batches = []
        for i in range(num_batches):
            start_idx = i * self.batch_size
            end_idx = min((i + 1) * self.batch_size, num_files)
            batch = self.cif_files[start_idx:end_idx]
            batches.append(batch)
            
            # Save batch to disk
            batch_df = pd.DataFrame({'file_path': batch})
            batch_df.to_csv(os.path.join(self.batch_dir, f'batch_{i+1}.csv'), index=False)
        
        return batches
    
    def get_batch_files(self, batch_id: int) -> List[str]:
        """Get file paths for a specific batch"""
        batch_file = os.path.join(self.batch_dir, f'batch_{batch_id}.csv')
        
        if not os.path.exists(batch_file):
            raise ValueError(f"Batch file not found: {batch_file}")
        
        batch_df = pd.read_csv(batch_file)
        return batch_df['file_path'].tolist()
    
    def get_num_batches(self) -> int:
        """Get the total number of batches"""
        return math.ceil(len(self.cif_files) / self.batch_size)
