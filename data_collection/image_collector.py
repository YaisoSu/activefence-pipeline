import json
import os
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Any
import requests
from PIL import Image
import yaml
from tqdm import tqdm
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ImageCollector:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.output_path = self.config['data_collection']['images']['output_path']
        self.metadata_path = self.config['data_collection']['images']['metadata_path']
        self.max_samples = self.config['data_collection']['images']['max_samples']
        
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.metadata_path, exist_ok=True)
        
        self.collected_metadata = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_image_hash(self, image_content: bytes) -> str:
        """Generate a hash for the image content."""
        return hashlib.md5(image_content).hexdigest()
    
    def save_image(self, image_content: bytes, image_id: str, extension: str = 'jpg') -> str:
        """Save image to disk and return the filepath."""
        filename = f"{image_id}.{extension}"
        filepath = os.path.join(self.output_path, filename)
        
        try:
            img = Image.open(BytesIO(image_content))
            img.verify()
            
            with open(filepath, 'wb') as f:
                f.write(image_content)
            
            return filepath
        except Exception as e:
            logger.error(f"Error saving image {image_id}: {e}")
            return None
    
    def collect_unsplash_images(self, limit: int = 3500) -> List[Dict[str, Any]]:
        """Collect images from Unsplash API."""
        logger.info(f"Collecting {limit} images from Unsplash...")
        metadata = []
        
        categories = ['nature', 'people', 'technology', 'architecture', 'food', 'travel', 'animals']
        images_per_category = limit // len(categories)
        
        for category in categories:
            for i in tqdm(range(images_per_category), desc=f"Unsplash {category}"):
                try:
                    url = f"https://source.unsplash.com/random/800x600/?{category}"
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        image_hash = self.get_image_hash(response.content)
                        image_id = f"unsplash_{category}_{image_hash[:8]}"
                        
                        filepath = self.save_image(response.content, image_id)
                        if filepath:
                            meta = {
                                'id': image_id,
                                'source': 'unsplash',
                                'url': response.url,
                                'category': category,
                                'filepath': filepath,
                                'file_size': len(response.content),
                                'timestamp': datetime.utcnow().isoformat(),
                                'metadata': {
                                    'platform': 'unsplash',
                                    'search_term': category,
                                    'license': 'Unsplash License',
                                    'attribution_required': True
                                }
                            }
                            metadata.append(meta)
                    
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error collecting Unsplash image: {e}")
                    continue
        
        return metadata
    
    def collect_picsum_images(self, limit: int = 3500) -> List[Dict[str, Any]]:
        """Collect images from Lorem Picsum."""
        logger.info(f"Collecting {limit} images from Lorem Picsum...")
        metadata = []
        
        for i in tqdm(range(limit), desc="Lorem Picsum"):
            try:
                width = 800 + (i % 400)
                height = 600 + (i % 300)
                url = f"https://picsum.photos/{width}/{height}"
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    image_hash = self.get_image_hash(response.content)
                    image_id = f"picsum_{image_hash[:8]}"
                    
                    filepath = self.save_image(response.content, image_id)
                    if filepath:
                        meta = {
                            'id': image_id,
                            'source': 'picsum',
                            'url': response.url,
                            'dimensions': f"{width}x{height}",
                            'filepath': filepath,
                            'file_size': len(response.content),
                            'timestamp': datetime.utcnow().isoformat(),
                            'metadata': {
                                'platform': 'Lorem Picsum',
                                'image_type': 'placeholder',
                                'license': 'Creative Commons CC0',
                                'attribution_required': False
                            }
                        }
                        metadata.append(meta)
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error collecting Picsum image: {e}")
                continue
        
        return metadata
    
    def collect_placeholder_images(self, limit: int = 3000) -> List[Dict[str, Any]]:
        """Collect images from various placeholder services."""
        logger.info(f"Collecting {limit} placeholder images...")
        metadata = []
        
        services = [
            {'name': 'placeholder', 'url': 'https://via.placeholder.com/{size}', 'sizes': ['640x480', '800x600', '1024x768']},
            {'name': 'placeimg', 'url': 'https://placeimg.com/{width}/{height}/any', 'sizes': [(640, 480), (800, 600), (1024, 768)]}
        ]
        
        images_per_service = limit // len(services)
        
        for service in services:
            for i in tqdm(range(images_per_service), desc=f"{service['name']} images"):
                try:
                    if service['name'] == 'placeholder':
                        size = service['sizes'][i % len(service['sizes'])]
                        url = service['url'].format(size=size)
                    else:
                        width, height = service['sizes'][i % len(service['sizes'])]
                        url = service['url'].format(width=width, height=height)
                    
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        image_hash = self.get_image_hash(response.content)
                        image_id = f"{service['name']}_{image_hash[:8]}"
                        
                        filepath = self.save_image(response.content, image_id)
                        if filepath:
                            meta = {
                                'id': image_id,
                                'source': service['name'],
                                'url': url,
                                'filepath': filepath,
                                'file_size': len(response.content),
                                'timestamp': datetime.utcnow().isoformat(),
                                'metadata': {
                                    'platform': service['name'],
                                    'image_type': 'placeholder',
                                    'license': 'Free to use',
                                    'attribution_required': False
                                }
                            }
                            metadata.append(meta)
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error collecting from {service['name']}: {e}")
                    continue
        
        return metadata
    
    def save_metadata(self, metadata: List[Dict[str, Any]]):
        """Save image metadata to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.metadata_path, f"image_metadata_{timestamp}.json")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved metadata for {len(metadata)} images to {filename}")
    
    def collect_all(self):
        """Collect images from all sources."""
        logger.info("Starting image data collection...")
        
        unsplash_data = self.collect_unsplash_images(35)
        self.collected_metadata.extend(unsplash_data)
        
        picsum_data = self.collect_picsum_images(35)
        self.collected_metadata.extend(picsum_data)
        
        placeholder_data = self.collect_placeholder_images(30)
        self.collected_metadata.extend(placeholder_data)
        
        self.save_metadata(self.collected_metadata)
        
        logger.info(f"Collection complete! Total images: {len(self.collected_metadata)}")
        
        stats = {
            'total_images': len(self.collected_metadata),
            'sources': {
                'unsplash': len([d for d in self.collected_metadata if d['source'] == 'unsplash']),
                'picsum': len([d for d in self.collected_metadata if d['source'] == 'picsum']),
                'placeholder': len([d for d in self.collected_metadata if 'placeholder' in d['source']])
            },
            'total_size_mb': sum(d['file_size'] for d in self.collected_metadata) / (1024 * 1024),
            'collection_date': datetime.now().isoformat()
        }
        
        stats_file = os.path.join(self.metadata_path, "collection_stats.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)


if __name__ == "__main__":
    collector = ImageCollector()
    collector.collect_all()