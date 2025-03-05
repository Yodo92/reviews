
service = "config-manager"
DSN = env.str("POSTGRESQL_DSN")
pg_pool = asyncpg.request_pool(DSN, service, max_size=2)


class ConfigManager:
    """This is a class that I'd like to use in my business logic
    I instantiate an instance of this class and call necessary methods to retrieve a certain data from my db (postgres)
    it has i DI called repo to run the query"""
    def __init__(self, repo: Repository):
        self.repo = repo

    async def get_all(self) -> list[RPConfig]:
        config: list[RPConfig] = await self.repo.get_all()
        return config

    async def get_by_id(self, config: RPConfig) -> RPConfig:
        config = await self.repo.get_by_id(config)
        return config

    async def create(self, config: RPConfig) -> RPConfig:
        config = await self.repo.create(config)
        return config

    async def update(self, config: RPConfig) -> RPConfig:
        config = await self.repo.update(config)
        return config

    async def delete(self, config: RPConfig) -> bool:
        config = await self.repo.delete(config)
        return bool(config)


class ReflinksManager:
    def __init__(self, repo: Repository):
        self.repo = repo

    async def create(self, item: Reflinks) -> Reflinks:
        item = await self.repo.create(item)
        return item

    async def get_referees_for_sum_check(self) -> list[str]:
        item = await self.repo.get_referees_for_sum_check()
        return item

    async def get_referrers_for_reward(self, item: list[str]) -> list[str]:
        item = await self.repo.get_referrers_for_reward(item)
        return item

    async def set_is_rewarded(self, item: list[str]) -> bool:
        item = await self.repo.set_is_rewarded(item)
        return item

# these are instances that I use in my app to retrieve data from db, I instanciate them here and import wherever I want
config_manager = ConfigManager(RPConfigRepository(pg_pool))

reflinks_manager = ReflinksManager(ReflinksRepository(pg_pool))

== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

class Repository[T](ABC):
"""This is an abstract class to set an interface for all repositories"""

    @abstractmethod
    async def get_all(self) -> T:
        """Retrieves all entities"""

    @abstractmethod
    async def get_by_id(self, model: T) -> T:
        """Retrieves an entity by id"""

    @abstractmethod
    async def create(self, model: T) -> T:
        """Creates a new entity"""

    @abstractmethod
    async def update(self, model: T) -> T:
        """Updates an entity"""

    @abstractmethod
    async def delete(self, model: T) -> bool:
        """Deletes an entity"""

== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == =

class Reflinks(BaseModelMixin):
    """This is a model that represents a table reflinks in my db """
    id: int | None = None
    referrer_profile_uuid: UUID
    referee_profile_uuid: UUID
    invitation_date: datetime | None = None
    is_awarded: bool | None = None

class ServiceId(IntEnum):
    offer = 1
    loyalty = 2


class RPConfig(BaseModel):
    """This is a model that represents a table rpconfig in my db """
    id: bool | None = None
    draft: bool
    title: str
    rp_start_date: date
    rp_end_date: date
    prefix: str = Field(default="R")
    postfix_length: int = Field(default=12)
    min_sum: int
    attempts_limit: int
    time_limit: int = Field(default=1440)
    referrer_award_id: int
    referee_award_id: int
    referrer_service_id: ServiceId
    referee_service_id: ServiceId
    text: str
    rules_link: str
    app_link: dict  # TODO: specify
    images: dict
    created_at: datetime
    updated_at: datetime
    meta: dict

== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

class ReflinksRepository(Repository[Reflinks]):
    """This is a concrete repository that has a DI of connection (pg_pool) to my db
    this is where all my queries are implemented, it is parameterized via Reflinks model"""
    def __init__(self, pg_pool: PoolManager) -> None:
        self.pg_pool = pg_pool

    async def get_all(self):
        pass

    async def get_by_id(self, model: Reflinks) -> Reflinks:
        pass

    async def get_referees_for_sum_check(self) -> list[str]:
        """
        Retrieve referee_profile_uuid to check if they spent min_sum
        """
        records: asyncpg.Record = await self.pg_pool.pool.fetch(
            """
                SELECT  referee_profile_uuid FROM ref_links WHERE is_rewarded IS NULL;
            """
        )
        return [str(record["referee_profile_uuid"]) for record in records]

    async def get_referrers_for_reward(self, referee_ids: list[str]) -> list[str]:
        """
        Retrieve referral_profile_uuids to reward with an offer or bonuses
        """
        records: asyncpg.Record = await self.pg_pool.pool.fetch(
            """
                SELECT referrer_profile_uuid
                FROM ref_links
                WHERE referee_profile_uuid = ANY($1) AND is_rewarded IS NULL;
            """,
            referee_ids,
        )
        return [str(record["referrer_profile_uuid"]) for record in records]

    async def set_is_rewarded(self, referrers_succeeded: list[str]) -> None:
        """
        Set is_rewarded flag to true
        """
        await self.pg_pool.pool.execute(
            """
               UPDATE ref_links
                SET is_rewarded = TRUE
                WHERE referrer_profile_uuid = ANY($1)
                AND is_rewarded IS NULL;
            """,
            referrers_succeeded,
        )

    async def create(self, item: Reflinks) -> Reflinks:
        record: asyncpg.Record = await self.pg_pool.pool.fetchrow(
            """
                INSERT INTO ref_links (
                referrer_profile_uuid,
                referee_profile_uuid
                )
                VALUES ($1, $2) RETURNING *;
            """,
            item.referrer_profile_uuid,
            item.referee_profile_uuid,
        )
        item = Reflinks.model_validate(record)
        return item

    async def update(self, model: Reflinks) -> Reflinks:
        pass

    async def delete(self, model: Reflinks) -> Reflinks:
        pass


