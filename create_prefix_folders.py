#!/usr/bin/env python3
"""
Script to create folders for each prefix in the prefix_dictionary.yaml file.
Creates folders in data/clean, data/raw, dictionary_files/clean, and dictionary_files/raw directories.
"""

import os
import yaml
from pathlib import Path


def load_prefix_dictionary(yaml_file_path):
    """Load the prefix dictionary from YAML file."""
    try:
        with open(yaml_file_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: File '{yaml_file_path}' not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None


def create_prefix_folders(prefix_dict, base_dirs=None):
    """Create folders for each prefix in the specified base directories."""
    if base_dirs is None:
        base_dirs = ['data/clean', 'data/raw', 'dictionary_files/clean', 'dictionary_files/raw']
    
    if not prefix_dict:
        print("No prefix dictionary loaded.")
        return
    
    # Extract prefixes (top-level keys from YAML)
    prefixes = [key for key in prefix_dict.keys() if not key.startswith('#')]
    
    print(f"Found {len(prefixes)} prefixes in dictionary:")
    for prefix in sorted(prefixes):
        print(f"  - {prefix}")
    
    # Create base directories if they don't exist
    for base_dir in base_dirs:
        Path(base_dir).mkdir(parents=True, exist_ok=True)
        print(f"\nEnsured base directory exists: {base_dir}")
    
    # Create folders for each prefix in each base directory
    created_folders = []
    existing_folders = []
    
    for base_dir in base_dirs:
        for prefix in prefixes:
            folder_path = Path(base_dir) / prefix
            
            if folder_path.exists():
                existing_folders.append(str(folder_path))
            else:
                folder_path.mkdir(parents=True, exist_ok=True)
                created_folders.append(str(folder_path))
    
    # Report results
    if created_folders:
        print(f"\nCreated {len(created_folders)} new folders:")
        for folder in sorted(created_folders):
            print(f"  ✓ {folder}")
    
    if existing_folders:
        print(f"\nSkipped {len(existing_folders)} existing folders:")
        for folder in sorted(existing_folders):
            print(f"  - {folder}")
    
    print(f"\nTotal prefixes processed: {len(prefixes)}")
    print(f"Total directories created: {len(prefixes) * len(base_dirs)}")


def main():
    """Main function to orchestrate the folder creation process."""
    # Path to the prefix dictionary YAML file
    yaml_file_path = 'dictionary_files/prefix_dictionary.yaml'
    
    print("GNMA Data Manager - Prefix Folder Creator")
    print("=" * 45)
    print(f"Loading prefix dictionary from: {yaml_file_path}")
    
    # Load the prefix dictionary
    prefix_dict = load_prefix_dictionary(yaml_file_path)
    
    if prefix_dict:
        # Create folders for each prefix
        create_prefix_folders(prefix_dict)
        
        print("\n🎉 Folder creation process completed successfully!")
        print("✓ Folders created for all prefixes in:")
        print("  - data/clean")
        print("  - data/raw") 
        print("  - dictionary_files/clean")
        print("  - dictionary_files/raw")
    else:
        print("Failed to load prefix dictionary. Exiting.")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 