#!/usr/bin/env python
"""
LLM Integration for Warehouse Optimiser
---------------------------------------
This script provides utilities to call LLM APIs for warehouse sizing decisions.
"""

import argparse
import json
import os
import sys
import logging
import requests
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("llm_integration")

class LLMClient:
    """Client for making LLM API calls for warehouse sizing."""
    
    def __init__(self, api_key: str, model: str = "gpt-4", endpoint: str = None):
        self.api_key = api_key
        self.model = model
        self.endpoint = endpoint or "https://api.openai.com/v1/chat/completions"
    
    def predict_warehouse_size(self, features: Dict[str, Any]) -> str:
        """
        Predict optimal warehouse size based on query features.
        
        Args:
            features: Dictionary containing query execution statistics
            
        Returns:
            String representing warehouse size (xs, s, m, l, xl, etc.)
        """
        try:
            # Format prompt with features
            prompt = self._format_prompt(features)
            
            # Make API call
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are a data warehouse optimisation expert that recommends the optimal warehouse size."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 50
            }
            
            response = requests.post(self.endpoint, headers=headers, json=payload)
            response.raise_for_status()
            
            # Extract and validate warehouse size from response
            result = response.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # Extract warehouse size from response
            warehouse_size = self._extract_warehouse_size(content)
            return warehouse_size
            
        except Exception as e:
            logger.error(f"Error making LLM API call: {str(e)}")
            return "m"  # Default to medium if API call fails
    
    def _format_prompt(self, features: Dict[str, Any]) -> str:
        """Format the prompt with query features."""
        return f"""
Based on the following query execution statistics:
- Average execution time: {features.get('avg_execution_time', 0):.2f} ms
- Average rows processed: {features.get('avg_rows_processed', 0):,.0f} rows
- Average bytes scanned: {features.get('avg_bytes_scanned', 0):,.0f} bytes
- Current row count: {features.get('current_row_count', 0):,.0f} rows
- Is full refresh: {features.get('is_full_refresh', False)}
- Hour of day: {features.get('hour_of_day', 0)}
- Day of week: {features.get('day_of_week', 0)}

Recommend the optimal Snowflake warehouse size. Valid sizes are:
xs, s, m, l, xl, 2xl, 3xl, 4xl

Respond with ONLY the warehouse size as a single word, e.g., "m" or "xl".
"""
    
    def _extract_warehouse_size(self, content: str) -> str:
        """Extract warehouse size from LLM response."""
        valid_sizes = ["xs", "s", "m", "l", "xl", "2xl", "3xl", "4xl"]
        
        # Lower and clean the content
        clean_content = content.lower().strip()
        
        # Direct match check
        if clean_content in valid_sizes:
            return clean_content
        
        # Check for warehouse size pattern
        for size in valid_sizes:
            if size in clean_content:
                return size
        
        # Default fallback
        logger.warning(f"Could not extract valid warehouse size from: {content}")
        return "m"


class RegressionModel:
    """Simple regression model for warehouse sizing."""
    
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            'intercept': 1.0,
            'avg_execution_time': 0.001,
            'avg_rows_processed': 0.000001, 
            'avg_bytes_scanned': 0.0000001,
            'current_row_count': 0.000002,
            'is_full_refresh': 1.0,
            'hour_of_day_factor': 0.05,
            'day_of_week_factor': 0.05
        }
        
        self.warehouse_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl']
    
    def predict_warehouse_size(self, features: Dict[str, Any]) -> str:
        """
        Predict optimal warehouse size based on query features using regression.
        
        Args:
            features: Dictionary containing query execution statistics
            
        Returns:
            String representing warehouse size (xs, s, m, l, xl, etc.)
        """
        try:
            # Calculate score
            score = self.weights.get('intercept', 0)
            score += self.weights.get('avg_execution_time', 0) * features.get('avg_execution_time', 0)
            score += self.weights.get('avg_rows_processed', 0) * features.get('avg_rows_processed', 0)
            score += self.weights.get('avg_bytes_scanned', 0) * features.get('avg_bytes_scanned', 0)
            score += self.weights.get('current_row_count', 0) * features.get('current_row_count', 0)
            
            if features.get('is_full_refresh', False):
                score += self.weights.get('is_full_refresh', 0)
            
            # Map score to warehouse size index
            index = min(max(int(score / 10), 0), len(self.warehouse_sizes) - 1)
            return self.warehouse_sizes[index]
            
        except Exception as e:
            logger.error(f"Error in regression prediction: {str(e)}")
            return "m"  # Default to medium if prediction fails


def main():
    """Main entry point for command line execution."""
    parser = argparse.ArgumentParser(description="LLM Integration for Warehouse Optimiser")
    parser.add_argument("--features", required=True, help="JSON string of query features")
    parser.add_argument("--model", choices=["llm", "regression"], default="regression", 
                        help="Model type to use for prediction")
    parser.add_argument("--api-key", help="API key for LLM service")
    parser.add_argument("--weights", help="JSON string of regression weights")
    
    args = parser.parse_args()
    
    try:
        # Parse features
        features = json.loads(args.features)
        
        if args.model == "llm" and args.api_key:
            # Use LLM model
            client = LLMClient(api_key=args.api_key)
            warehouse_size = client.predict_warehouse_size(features)
        else:
            # Use regression model
            weights = json.loads(args.weights) if args.weights else None
            model = RegressionModel(weights=weights)
            warehouse_size = model.predict_warehouse_size(features)
        
        # Output result
        print(warehouse_size)
        return 0
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print("m")  # Default fallback
        return 1


if __name__ == "__main__":
    sys.exit(main()) 