"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API, prints each astronaut's name and flying craft, and
generates a summary report grouped by craft.

There are three tasks:
1. Get astronaut data from the API
2. Print each astronaut's details using dynamic task mapping
3. Generate a summary report with astronaut counts per craft

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://www.astronomer.io/docs/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow.sdk import Asset, dag, task
from pendulum import datetime
import requests
from collections import defaultdict


@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    @task(
        outlets=[Asset("current_astronauts")]
    )
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception:
            print("API currently not available, using hardcoded data instead.")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Tom Kononenko"},
                {"craft": "ISS", "name": "Nikolai Chub"},
                {"craft": "ISS", "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS", "name": "Matthew Dominick"},
                {"craft": "ISS", "name": "Michael Barratt"},
                {"craft": "ISS", "name": "Jeanette Epps"},
                {"craft": "ISS", "name": "Alexander Grebenkin"},
                {"craft": "ISS", "name": "Butch Wilmore"},
                {"craft": "ISS", "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    @task(
        outlets=[Asset("astronaut_summary_report")]
    )
    def generate_summary_report(astronauts: list[dict]) -> dict:
        """
        Generates a summary report of astronauts grouped by craft,
        including counts and crew member names per craft.
        """
        craft_groups = defaultdict(list)
        for person in astronauts:
            craft_groups[person["craft"]].append(person["name"])

        craft_summary = [
            {"craft": craft, "count": len(names), "crew_members": names}
            for craft, names in craft_groups.items()
        ]

        report = {
            "total_astronauts": len(astronauts),
            "total_crafts": len(craft_groups),
            "craft_summary": craft_summary,
        }

        for craft_info in craft_summary:
            print(
                f"{craft_info['craft']}: {craft_info['count']} astronauts "
                f"- {', '.join(craft_info['crew_members'])}"
            )

        print(f"\nTotal: {report['total_astronauts']} astronauts across {report['total_crafts']} crafts")

        return report

    astronauts = get_astronauts()

    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronauts
    )

    generate_summary_report(astronauts=astronauts)


example_astronauts()
