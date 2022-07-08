"""
This script uses Apache Beam to build a pipeline to check the top N StackOverflow
answers for broken links, saving the results to a JSON file.

Usage (running locally):
1. Ensure the GOOGLE_APPLICATION_CREDENTIALS environment variable is set
2. Run locally with the following command:
    python3 stack-overflow.py \
        --region us-east1 \
        --project yourproject-1234 \
        --direct_num_workers 1 \
        --runner DirectRunner \
        --temp_location gs://your-bucket/tmp \
        --output ./so-beam-results \
        --num_answers 100
3. Then two output files will be produced in your current working directory.
   One file contains results from all URLs, the other contains results from
   only the broken links:
        a. so-beam-results-2022-07-07T22:26:50-all_responses-00000-of-00001.json
        b. so-beam-results-2022-07-07T22:26:50-failures-00000-of-00001.json

Usage (running on Google Cloud Dataflow)
1. Ensure the GOOGLE_APPLICATION_CREDENTIALS environment variable is set
2. Run on Dataflow with the following command:
    python3 stack-overflow.py \
        --region us-east1 \
        --project yourprojeect-1234 \
        --runner DataflowRunner \
        --temp_location gs://your-bucket/tmp \
        --num_workers 1 \
        --output gs://your-bucket/test-results \
        --num_answers 100
   The difference is that the runner should be DataflowRunenr and the output should
   go to a Google Cloud bucket instead of a local file.
3. The two output files will be in the bucket you specified under --output

Some code in this script is adapted from the Apache Beam wordcount example:
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
"""

import re
import time
import json
import logging
import requests
import argparse
import apache_beam as beam
from urllib.parse import urlparse
from dataclasses import dataclass
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from typing import List, Tuple, Optional


@dataclass
class AnswerWithURL:
    score: int
    answer_id: int
    question_title: str
    answer_url: str

    def to_dict(self):
        return {
            "score": self.score,
            "answer_id": self.answer_id,
            "question_title": self.question_title,
            "answer_url": self.answer_url
        }


@dataclass
class AnswerURLCheckResult:
    url: str
    answerURLs: List[AnswerWithURL]
    status: Optional[int]
    err_type: Optional[str]
    err_details: Optional[str]
    req_time_secs: Optional[float]

    def to_dict(self):
        return {
            "answer_score_sum": sum(a.score for a in self.answerURLs),
            "answer_count": len(self.answerURLs),
            "url": self.url,
            "answers": [a.to_dict() for a in self.answerURLs],
            "status": self.status,
            "err_type": self.err_type,
            "err_details": self.err_details,
            "req_time_secs": self.req_time_secs,
        }


class ExtractURLsFromAnswer(beam.DoFn):
    def process(self, element, *args, **kwargs):
        seen_urls = set()

        # The bodies in stack overflow answers are stored as HTML, so links appear like <a href="..."> elements.
        # This crude regex looks for links that start with "http" or "https".
        for match in re.finditer(r'<a href="(https?[^"]+)', element["body"]):
            url = match.group(1)

            # If the same URL is in one question multiple times, we only want to add it to the pipeline once
            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Ignore stackoverflow.com URLs because there are too many to check them all without getting rate-limited.
            # These are also unlikely to be broken.
            try:
                parsed_url = urlparse(url)
                if parsed_url.netloc.endswith("stackoverflow.com"):
                    logging.info("Skipping %s", url)
                    continue
            except Exception as e:
                # If the URL can't be parsed, we still keep it in the pipeline. It will fail again at a later
                # stage. We want to track broken URLs so can't discard this URL here.
                logging.info("Failed to parse URL %s, error %s", url, str(e))

            answer_with_url = AnswerWithURL(score=element["score"], answer_id=element["answer_id"],
                                            question_title=element["question_title"],
                                            answer_url="https://stackoverflow.com/a/{}".format(element["answer_id"]))
            yield url, answer_with_url


class CheckURLReturns200(beam.DoFn):
    def process(self, element: Tuple[str, List[AnswerWithURL]], *args, **kwargs):
        url, answer_urls = element

        url_check_result = AnswerURLCheckResult(url=url, answerURLs=answer_urls, status=None, err_type=None,
                                                err_details=None, req_time_secs=None)

        try:
            logging.info("Making HEAD request to %s", url)
            connect_timeout = 5.0
            read_timeout = 10.0
            req_headers = {
                "User-Agent": "broken-link-checker"
            }
            resp = requests.head(url, timeout=(connect_timeout, read_timeout), headers=req_headers)
            url_check_result.status = resp.status_code
            url_check_result.req_time_secs = resp.elapsed.total_seconds()
            resp.raise_for_status()
        except Exception as e:
            logging.info("Got error %s from %s", str(e), url)
            url_check_result.err_details = str(e)
            url_check_result.err_type = type(e).__name__

        yield url_check_result


def is_failure(element: AnswerURLCheckResult):
    return element.err_type is not None


def combine_pcollection_to_list(els):
    ret = []
    for el in els:
        if isinstance(el, list):
            ret.extend(el)
        else:
            ret.append(el)
    return ret


def json_encode(check_results: List[AnswerURLCheckResult]):
    results_as_dicts = []

    for check_result in check_results:
        results_as_dicts.append(check_result.to_dict())

    results_as_dicts.sort(key=lambda x: x["answer_score_sum"], reverse=True)
    return json.dumps(results_as_dicts, indent=4)


class JsonifyAnswerURLCheckResults(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'CombineAllElements' >> beam.CombineGlobally(combine_pcollection_to_list)
                | 'JSONEncode' >> beam.Map(json_encode)
                )


def drop_key(element: Tuple[str, AnswerURLCheckResult]):
    url, result = element
    return result


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--num_answers',
        dest='num_answers',
        default=50,
        help='Number of stack overflow answers to check links in')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    query = """
    select questions.title as question_title, answers.body, answers.score, answers.id as answer_id from `bigquery-public-data.stackoverflow.posts_answers` as answers
    join `bigquery-public-data.stackoverflow.posts_questions` as questions
    on answers.parent_id = questions.id
    where answers.body like '%<a href=%' order by answers.score desc limit {} 
    """.format(int(known_args.num_answers))

    with beam.Pipeline(options=pipeline_options) as p:
        time_str = time.strftime("%Y-%m-%dT%H:%M:%S")
        failures_file_path = "{}-{}-failures".format(known_args.output, time_str)
        all_responses_file_path = "{}-{}-all_responses".format(known_args.output, time_str)
        logging.info("Will write failures to %s", failures_file_path)
        logging.info("Will write all to %s", all_responses_file_path)

        responses = (
                p
                | 'Query' >> ReadFromBigQuery(query=query, use_standard_sql=True,
                                              method=ReadFromBigQuery.Method.DIRECT_READ)
                | 'ExtractURLs' >> beam.ParDo(ExtractURLsFromAnswer())
                | 'GroupByURL' >> beam.CombinePerKey(combine_pcollection_to_list)
                | 'CheckURLAlive' >> beam.ParDo(CheckURLReturns200())
        )

        failures = (
                responses
                | 'ExtractFailures' >> beam.Filter(is_failure)
                | 'JsonifyFailures' >> JsonifyAnswerURLCheckResults()
                | "WriteFailures" >> WriteToText(file_path_prefix=failures_file_path, file_name_suffix=".json",
                                                 num_shards=1)
        )

        all_responses = (
                responses
                | 'JsonifyAll' >> JsonifyAnswerURLCheckResults()
                | 'WriteAll' >> WriteToText(file_path_prefix=all_responses_file_path, file_name_suffix=".json",
                                            num_shards=1)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
