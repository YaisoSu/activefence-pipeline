"""
Text Data Collector for ActiveFence Pipeline

This module collects text data from various online sources including:
- Wikipedia articles
- News RSS feeds
- Reddit posts

The collected data is saved in JSON format with metadata for further processing.
"""

import json
import os
import time
import random
from datetime import datetime
from typing import Dict, List, Any
import requests
from bs4 import BeautifulSoup
import yaml
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TextCollector:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.output_path = self.config['data_collection']['text']['output_path']
        self.max_samples = self.config['data_collection']['text']['max_samples']
        os.makedirs(self.output_path, exist_ok=True)
        
        self.collected_data = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def collect_wikipedia_articles(self, limit: int = 3000) -> List[Dict[str, Any]]:
        """Collect random Wikipedia articles."""
        logger.info(f"Collecting {limit} Wikipedia articles...")
        articles = []
        
        for _ in tqdm(range(limit), desc="Wikipedia articles"):
            try:
                random_url = "https://en.wikipedia.org/api/rest_v1/page/random/summary"
                response = self.session.get(random_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    article = {
                        'id': f"wiki_{data.get('pageid', '')}",
                        'source': 'wikipedia',
                        'title': data.get('title', ''),
                        'text': data.get('extract', ''),
                        'url': data.get('content_urls', {}).get('desktop', {}).get('page', ''),
                        'timestamp': datetime.utcnow().isoformat(),
                        'author': 'Wikipedia Contributors',
                        'metadata': {
                            'type': 'encyclopedia',
                            'language': 'en',
                            'description': data.get('description', '')
                        }
                    }
                    articles.append(article)
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error collecting Wikipedia article: {e}")
                continue
        
        return articles
    
    def collect_news_articles(self, limit: int = 3500) -> List[Dict[str, Any]]:
        """Collect news articles from various RSS feeds."""
        logger.info(f"Collecting {limit} news articles...")
        articles = []
        
        rss_feeds = [
            "https://feeds.bbci.co.uk/news/rss.xml",
            "https://rss.cnn.com/rss/edition.rss",
            "https://feeds.reuters.com/reuters/topNews",
            "https://www.theguardian.com/world/rss",
            "https://feeds.npr.org/1001/rss.xml"
        ]
        
        articles_per_feed = limit // len(rss_feeds)
        
        for feed_url in rss_feeds:
            try:
                response = self.session.get(feed_url, timeout=10)
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')[:articles_per_feed]
                
                for item in tqdm(items, desc=f"News from {feed_url.split('/')[2]}"):
                    article = {
                        'id': f"news_{hash(item.find('link').text if item.find('link') else '')}",
                        'source': feed_url.split('/')[2],
                        'title': item.find('title').text if item.find('title') else '',
                        'text': item.find('description').text if item.find('description') else '',
                        'url': item.find('link').text if item.find('link') else '',
                        'timestamp': item.find('pubDate').text if item.find('pubDate') else datetime.utcnow().isoformat(),
                        'author': item.find('author').text if item.find('author') else 'Unknown',
                        'metadata': {
                            'type': 'news',
                            'language': 'en',
                            'category': item.find('category').text if item.find('category') else 'general'
                        }
                    }
                    articles.append(article)
                
            except Exception as e:
                logger.error(f"Error collecting from {feed_url}: {e}")
                continue
        
        return articles
    
    def collect_reddit_posts(self, limit: int = 3500) -> List[Dict[str, Any]]:
        """Collect Reddit posts from various subreddits."""
        logger.info(f"Collecting {limit} Reddit posts...")
        posts = []
        
        subreddits = [
            'worldnews', 'technology', 'science', 'programming',
            'dataisbeautiful', 'todayilearned', 'explainlikeimfive'
        ]
        
        posts_per_subreddit = limit // len(subreddits)
        
        for subreddit in subreddits:
            try:
                url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit=100"
                headers = {'User-Agent': 'ActiveFence Data Collector 1.0'}
                response = self.session.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    items = data['data']['children'][:posts_per_subreddit]
                    
                    for item in tqdm(items, desc=f"Reddit r/{subreddit}"):
                        post_data = item['data']
                        post = {
                            'id': f"reddit_{post_data['id']}",
                            'source': f'reddit_r_{subreddit}',
                            'title': post_data.get('title', ''),
                            'text': post_data.get('selftext', '') or post_data.get('title', ''),
                            'url': f"https://reddit.com{post_data.get('permalink', '')}",
                            'timestamp': datetime.fromtimestamp(post_data.get('created_utc', 0)).isoformat(),
                            'author': post_data.get('author', 'Unknown'),
                            'metadata': {
                                'type': 'social_media',
                                'platform': 'reddit',
                                'subreddit': subreddit,
                                'score': post_data.get('score', 0),
                                'num_comments': post_data.get('num_comments', 0),
                                'is_video': post_data.get('is_video', False)
                            }
                        }
                        posts.append(post)
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error collecting from r/{subreddit}: {e}")
                continue
        
        return posts
    
    def save_data(self, data: List[Dict[str, Any]]):
        """Save collected data to JSON files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.output_path, f"text_data_{timestamp}.json")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(data)} text samples to {filename}")
    
    def collect_all(self):
        """Collect text data from all sources."""
        logger.info("Starting text data collection...")
        
        wikipedia_data = self.collect_wikipedia_articles(30)
        self.collected_data.extend(wikipedia_data)
        
        news_data = self.collect_news_articles(35)
        self.collected_data.extend(news_data)
        
        reddit_data = self.collect_reddit_posts(35)
        self.collected_data.extend(reddit_data)
        
        random.shuffle(self.collected_data)
        
        self.save_data(self.collected_data)
        
        logger.info(f"Collection complete! Total samples: {len(self.collected_data)}")
        
        stats = {
            'total_samples': len(self.collected_data),
            'sources': {
                'wikipedia': len([d for d in self.collected_data if d['source'] == 'wikipedia']),
                'news': len([d for d in self.collected_data if 'news' in d['id']]),
                'reddit': len([d for d in self.collected_data if 'reddit' in d['id']])
            },
            'collection_date': datetime.now().isoformat()
        }
        
        stats_file = os.path.join(self.output_path, "collection_stats.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)


if __name__ == "__main__":
    collector = TextCollector()
    collector.collect_all()