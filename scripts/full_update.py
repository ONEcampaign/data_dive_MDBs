from scripts.intel_tracker.heatmap_tracker import heatmap_data
from scripts.world_bank_votes.download_shares import votes_chart_data
from scripts.logger import logger

if __name__ == "__main__":
    logger.info("Triggered full update")
    heatmap_data()
    votes_chart_data()
