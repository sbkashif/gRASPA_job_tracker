import argparse
from .config_parser import ConfigParser
from .job_tracker import JobTracker
from .utils import create_default_config, check_slurm_available

def main():
    parser = argparse.ArgumentParser(description='Run GRASPA job tracker')
    parser.add_argument('--config', '-c', type=str, required=True,
                        help='Path to configuration file')
    parser.add_argument('--polling-interval', '-p', type=int, default=60,
                        help='Polling interval in seconds (default: 60)')
    parser.add_argument('--create-default-config', action='store_true',
                        help='Create a default configuration file')
    
    args = parser.parse_args()
    
    if args.create_default_config:
        create_default_config(args.config)
        print(f"Created default configuration at {args.config}")
        print("Please edit this file with your specific settings before running again.")
        return
    
    # Check if SLURM is available
    if not check_slurm_available():
        print("ERROR: SLURM commands not found. Make sure you are on a system with SLURM installed.")
        return
    
    # Load and validate configuration
    try:
        config_parser = ConfigParser(args.config)
        config = config_parser.get_config()
    except (FileNotFoundError, ValueError) as e:
        print(f"ERROR: {e}")
        return
    
    # Create and run job tracker
    tracker = JobTracker(config)
    tracker.run(polling_interval=args.polling_interval)

if __name__ == '__main__':
    main()
