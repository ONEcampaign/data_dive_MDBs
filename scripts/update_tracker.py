from scripts.intel_tracker.heatmap_tracker import heatmap_data
from scripts.logger import logger

if __name__ == "__main__":
    logger.info("Triggered heatmap update")
    heatmap_data()
