# shared/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Notice we changed the protocol from postgresql:// to postgresql+asyncpg://
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/webhook_db"

# Create the Async Engine
engine = create_async_engine(DATABASE_URL, echo=False)

# Create the Async Session Factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine, 
    class_=AsyncSession, 
    expire_on_commit=False, 
    autocommit=False, 
    autoflush=False
)

async def get_db():
    """Async dependency function to yield a database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()