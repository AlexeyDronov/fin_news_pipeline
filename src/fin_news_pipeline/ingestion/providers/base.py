from abc import ABC, abstractmethod
from fin_news_pipeline.utils import RawArticle

class NewsProvider(ABC):
    @abstractmethod
    def fetch(self) -> list[RawArticle]:
        """Fetches a flat list of articles from the source."""
        pass