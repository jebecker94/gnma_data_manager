#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Pipeline Demos

Demonstration scripts for the combined GNMA data pipeline module.
Shows how to use the integrated downloader, formatter, parser, and processor
classes either individually or as a complete pipeline.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

from gnma_data_pipeline import (
    GNMAHistoricalDownloader, DownloadConfig,
    GNMADataFormatter, FormatterConfig,
    GNMADataParser,
    GNMADataProcessor, ProcessorConfig,
    GNMAPipeline,
    create_default_configs,
    create_pipeline_from_env
)


def demo_individual_components():
    """Demonstrate using individual pipeline components."""
    print("="*60)
    print("DEMO: Individual Pipeline Components")
    print("="*60)
    
    # Load environment variables
    load_dotenv()
    
    try:
        # 1. Downloader Demo
        print("\n1. DOWNLOADER COMPONENT:")
        download_config = DownloadConfig(
            email_value=os.getenv('email_value', 'demo@example.com'),
            id_value=os.getenv('id_value', 'demo_id'),
            user_agent=os.getenv('user_agent', 'Demo User Agent'),
            download_folder="data",
            schema_download_folder="dictionary_files"
        )
        
        downloader = GNMAHistoricalDownloader(download_config)
        print(f"   Downloader initialized with {len(downloader.prefix_dict)} prefixes")
        
        # 2. Formatter Demo  
        print("\n2. FORMATTER COMPONENT:")
        formatter_config = FormatterConfig(
            input_folder="data/raw",
            output_folder="data/raw",
            skip_existing=True
        )
        
        formatter = GNMADataFormatter(formatter_config)
        print("   Formatter initialized and ready for ZIP → Parquet conversion")
        
        # 3. Parser Demo
        print("\n3. PARSER COMPONENT:")
        parser = GNMADataParser(verbose=False)
        
        # Test format detection on a few prefixes
        test_prefixes = ['dailyllmni', 'monthlySFPS', 'monthly']
        print("   Format detection results:")
        for prefix in test_prefixes:
            try:
                analysis = parser.analyze_prefix_format(prefix)
                print(f"     {prefix:<15}: {analysis['format_type']}")
            except Exception as e:
                print(f"     {prefix:<15}: ERROR - {str(e)[:30]}...")
        
        # 4. Processor Demo
        print("\n4. PROCESSOR COMPONENT:")
        processor_config = ProcessorConfig(
            raw_folder="data/raw",
            clean_folder="data/clean",
            schema_folder="dictionary_files/combined"
        )
        
        processor = GNMADataProcessor(processor_config)
        print("   Processor initialized with integrated parser")
        
        print("\n✓ All individual components successfully initialized!")
        
    except Exception as e:
        print(f"Error in individual components demo: {e}")


def demo_complete_pipeline():
    """Demonstrate the complete integrated pipeline."""
    print("="*60)
    print("DEMO: Complete Integrated Pipeline")
    print("="*60)
    
    try:
        # Create pipeline from environment variables
        print("Creating complete pipeline from environment...")
        pipeline = create_pipeline_from_env()
        
        # Check pipeline status
        status = pipeline.get_pipeline_status()
        print("\nPipeline Status:")
        for component, state in status.items():
            print(f"  {component}: {state}")
        
        # Example workflow (without actually executing)
        print("\nExample workflow for 'monthlySFPS':")
        print("  1. Download: pipeline.downloader.download_prefix_data('monthlySFPS')")
        print("  2. Format:   pipeline.formatter.process_prefix_data('monthlySFPS')")
        print("  3. Process:  pipeline.processor.process_prefix_data('monthlySFPS')")
        print("  Or all at once: pipeline.process_complete_workflow('monthlySFPS')")
        
        print("\n✓ Complete pipeline successfully created!")
        
    except Exception as e:
        print(f"Error in complete pipeline demo: {e}")
        print("Make sure environment variables are set:")
        print("  - email_value")
        print("  - id_value")
        print("  - user_agent")


def demo_configuration_patterns():
    """Demonstrate different configuration patterns."""
    print("="*60)
    print("DEMO: Configuration Patterns")
    print("="*60)
    
    # 1. Default configurations
    print("\n1. DEFAULT CONFIGURATIONS:")
    try:
        download_config, formatter_config, processor_config = create_default_configs(
            email_value="demo@example.com",
            id_value="demo_id",
            user_agent="Demo User Agent"
        )
        
        print("   Default configs created for:")
        print(f"     Download folder: {download_config.download_folder}")
        print(f"     Format input:    {formatter_config.input_folder}")
        print(f"     Process clean:   {processor_config.clean_folder}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # 2. Custom configurations
    print("\n2. CUSTOM CONFIGURATIONS:")
    
    # High-performance config
    hp_formatter_config = FormatterConfig(
        input_folder="data/raw",
        skip_existing=True,
        batch_size=200,
        log_level="WARNING",
        validate_conversions=False
    )
    
    hp_processor_config = ProcessorConfig(
        raw_folder="data/raw",
        clean_folder="data/clean",
        skip_existing=True,
        log_level="WARNING",
        batch_size=100
    )
    
    print("   High-performance config:")
    print(f"     Formatter batch size: {hp_formatter_config.batch_size}")
    print(f"     Processor batch size: {hp_processor_config.batch_size}")
    print(f"     Log level: {hp_formatter_config.log_level}")
    
    # Debug config
    debug_formatter_config = FormatterConfig(
        input_folder="data/raw",
        skip_existing=False,
        log_level="DEBUG",
        validate_conversions=True
    )
    
    debug_processor_config = ProcessorConfig(
        raw_folder="data/raw",
        clean_folder="data/clean",
        skip_existing=False,
        log_level="DEBUG",
        validate_outputs=True
    )
    
    print("\n   Debug/thorough config:")
    print(f"     Skip existing: {debug_formatter_config.skip_existing}")
    print(f"     Log level: {debug_formatter_config.log_level}")
    print(f"     Validation: {debug_formatter_config.validate_conversions}")


def demo_workflow_scenarios():
    """Demonstrate different workflow scenarios."""
    print("="*60)
    print("DEMO: Workflow Scenarios")
    print("="*60)
    
    load_dotenv()
    
    try:
        # Create pipeline
        pipeline = create_pipeline_from_env()
        
        print("\n1. PARTIAL WORKFLOW (Format + Process only):")
        print("   # Skip download, start with existing ZIP files")
        print("   result = pipeline.process_complete_workflow(")
        print("       'monthlySFPS',")
        print("       download=False,")
        print("       format_data=True,")
        print("       process_data=True")
        print("   )")
        
        print("\n2. FORMAT ONLY WORKFLOW:")
        print("   # Just convert ZIP files to Parquet")
        print("   format_result = pipeline.formatter.process_prefix_data('dailyllmni')")
        print("   print(f'Converted {format_result.successful} files')")
        
        print("\n3. ANALYSIS WORKFLOW:")
        print("   # Analyze file formats before processing")
        print("   for prefix in ['dailyllmni', 'monthlySFPS', 'monthly']:")
        print("       analysis = pipeline.parser.analyze_prefix_format(prefix)")
        print("       print(f'{prefix}: {analysis[\"format_type\"]}')")
        
        print("\n4. BATCH PROCESSING:")
        print("   # Process multiple prefixes")
        print("   prefixes = ['monthlySFPS', 'monthlySFS', 'nimonSFPS']")
        print("   for prefix in prefixes:")
        print("       result = pipeline.process_complete_workflow(prefix)")
        print("       print(f'{prefix}: {result[\"process\"][\"status\"]}')")
        
        print("\n5. ERROR RECOVERY:")
        print("   # Handle failures gracefully")
        print("   try:")
        print("       result = pipeline.process_complete_workflow('invalid_prefix')")
        print("   except Exception as e:")
        print("       print(f'Workflow failed: {e}')")
        
    except Exception as e:
        print(f"Error in workflow scenarios demo: {e}")


def demo_integration_with_analyzer():
    """Demonstrate integration with the separate analyzer module."""
    print("="*60)
    print("DEMO: Integration with Schema Analyzer")
    print("="*60)
    
    try:
        # Import the separate analyzer
        from gnma_schema_analyzer import GNMASchemaAnalyzer
        from gnma_data_pipeline import GNMADataParser
        
        # Create parser and analyzer
        parser = GNMADataParser(verbose=False)
        analyzer = GNMASchemaAnalyzer(parser, verbose=False)
        
        print("1. ANALYZER INTEGRATION:")
        print("   from gnma_schema_analyzer import GNMASchemaAnalyzer")
        print("   from gnma_data_pipeline import GNMADataParser")
        print("   ")
        print("   parser = GNMADataParser()")
        print("   analyzer = GNMASchemaAnalyzer(parser)")
        
        print("\n2. ANALYSIS CAPABILITIES:")
        print("   # Comprehensive schema analysis")
        print("   analysis_df = analyzer.analyze_all_schemas()")
        print("   report = analyzer.generate_format_report()")
        print("   problems = analyzer.identify_problematic_schemas()")
        
        print("\n3. PIPELINE + ANALYSIS WORKFLOW:")
        print("   # 1. Use pipeline for data processing")
        print("   pipeline = create_pipeline_from_env()")
        print("   result = pipeline.process_complete_workflow('monthlySFPS')")
        print("   ")
        print("   # 2. Use analyzer for schema analysis")
        print("   analyzer = GNMASchemaAnalyzer(pipeline.parser)")
        print("   format_mapping = analyzer.create_format_mapping()")
        print("   validation_df = analyzer.validate_all_schemas()")
        
        print("\n✓ Pipeline and analyzer work together seamlessly!")
        
    except ImportError:
        print("Schema analyzer module not found - keeping analysis separate as requested")
    except Exception as e:
        print(f"Error in analyzer integration demo: {e}")


def demo_performance_considerations():
    """Demonstrate performance optimization patterns."""
    print("="*60)
    print("DEMO: Performance Optimization")
    print("="*60)
    
    print("1. CACHING STRATEGIES:")
    print("   # Parser has built-in caching")
    print("   parser = GNMADataParser()")
    print("   ")
    print("   # First analysis (populates cache)")
    print("   analysis1 = parser.analyze_prefix_format('dailyllmni')")
    print("   ")
    print("   # Second analysis (uses cache)")
    print("   analysis2 = parser.analyze_prefix_format('dailyllmni')")
    print("   ")
    print("   # Check cache stats")
    print("   stats = parser.get_cache_stats()")
    print("   print(f'Cached prefixes: {stats[\"cached_prefixes\"]}')")
    
    print("\n2. BATCH PROCESSING:")
    print("   # Configure for high throughput")
    print("   formatter_config = FormatterConfig(")
    print("       batch_size=200,")
    print("       skip_existing=True,")
    print("       log_level='WARNING',")
    print("       validate_conversions=False")
    print("   )")
    print("   ")
    print("   processor_config = ProcessorConfig(")
    print("       batch_size=100,")
    print("       skip_existing=True,")
    print("       validate_outputs=False")
    print("   )")
    
    print("\n3. MEMORY OPTIMIZATION:")
    print("   # Use Polars lazy loading in formatter")
    print("   # Process files in chunks rather than all at once")
    print("   # Clear parser cache periodically for long-running processes")
    print("   parser.clear_cache()  # Clear cache when needed")
    
    print("\n4. PARALLEL PROCESSING CONSIDERATIONS:")
    print("   # Each component is thread-safe for read operations")
    print("   # For parallel processing, create separate instances:")
    print("   # processor1 = GNMADataProcessor(config)")
    print("   # processor2 = GNMADataProcessor(config)")


def run_all_demos():
    """Run all available demos."""
    print("="*80)
    print("GNMA PIPELINE DEMO SUITE")
    print("="*80)
    
    demos = [
        ("Individual Components", demo_individual_components),
        ("Complete Pipeline", demo_complete_pipeline),
        ("Configuration Patterns", demo_configuration_patterns),
        ("Workflow Scenarios", demo_workflow_scenarios),
        ("Analyzer Integration", demo_integration_with_analyzer),
        ("Performance Considerations", demo_performance_considerations),
    ]
    
    for i, (name, demo_func) in enumerate(demos, 1):
        print(f"\n[{i}/{len(demos)}] {name}")
        print("-" * 60)
        try:
            demo_func()
        except Exception as e:
            print(f"Demo failed: {e}")
        print()


def main():
    """Main demo execution."""
    import sys
    
    if len(sys.argv) > 1:
        demo_type = sys.argv[1].lower()
        
        if demo_type == 'all':
            run_all_demos()
        elif demo_type == 'components':
            demo_individual_components()
        elif demo_type == 'pipeline':
            demo_complete_pipeline()
        elif demo_type == 'config':
            demo_configuration_patterns()
        elif demo_type == 'workflow':
            demo_workflow_scenarios()
        elif demo_type == 'analyzer':
            demo_integration_with_analyzer()
        elif demo_type == 'performance':
            demo_performance_considerations()
        else:
            print(f"Unknown demo type: {demo_type}")
            print("Available demos: all, components, pipeline, config, workflow, analyzer, performance")
    else:
        # Default demo
        print("="*80)
        print("GNMA PIPELINE - QUICK START DEMO")
        print("="*80)
        
        demo_individual_components()
        demo_complete_pipeline()
        
        print("\n" + "="*80)
        print("Quick Start Complete!")
        print("Run with 'all' argument to see all demos:")
        print("  python gnma_pipeline_demos.py all")
        print("Available demos: all, components, pipeline, config, workflow, analyzer, performance")


if __name__ == "__main__":
    main()