# Copyright (c) 2019, Frappe Technologies and contributors
# License: MIT. See LICENSE

import os

from rq.timeouts import JobTimeoutException

import frappe
from frappe import _
from custom_import_app.custom_import_app.doctype.data_import_custom.exporter import Exporter
from custom_import_app.custom_import_app.doctype.data_import_custom.importer import Importer, create_import_log
from frappe.model import CORE_DOCTYPES
from frappe.model.document import Document
from frappe.modules.import_file import import_file_by_path
from frappe.utils.background_jobs import enqueue, is_job_enqueued
from frappe.utils.csvutils import validate_google_sheets_url
import math


BLOCKED_DOCTYPES = CORE_DOCTYPES - {"User", "Role", "Print Format"}


class DataImportCustom(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		google_sheets_url: DF.Data | None
		import_file: DF.Attach | None
		import_type: DF.Literal["", "Insert New Records", "Update Existing Records"]
		mute_emails: DF.Check
		payload_count: DF.Int
		reference_doctype: DF.Link
		show_failed_logs: DF.Check
		status: DF.Literal["Pending","Preprocessing", "Running", "Success", "Partial Success", "Error", "Timed Out", "Stopped"]
		submit_after_import: DF.Check
		template_options: DF.Code | None
		template_warnings: DF.Code | None
	# end: auto-generated types

	def validate(self):
		doc_before_save = self.get_doc_before_save()
		if (
			not (self.import_file or self.google_sheets_url)
			or (doc_before_save and doc_before_save.import_file != self.import_file)
			or (doc_before_save and doc_before_save.google_sheets_url != self.google_sheets_url)
		):
			self.template_options = ""
			self.template_warnings = ""

		self.validate_doctype()
		# self.validate_import_file()
		self.validate_google_sheets_url()
		# self.set_payload_count()

	def validate_doctype(self):
		if self.reference_doctype in BLOCKED_DOCTYPES:
			frappe.throw(_("Importing {0} is not allowed.").format(self.reference_doctype))

	def validate_import_file(self):
		if self.import_file:
			# validate template
			self.get_importer()

	def validate_google_sheets_url(self):
		if not self.google_sheets_url:
			return
		validate_google_sheets_url(self.google_sheets_url)

	# def set_payload_count(self):
	# 	if self.import_file:
	# 		i = self.get_importer()
	# 		payloads = i.import_file.get_payloads_for_import()
	# 		self.payload_count = len(payloads)

	def set_payload_count(self):
		if self.import_file:
			importer = self.get_importer()
			data_iter = iter(importer.import_file.data)
			count = 0

			while True:
				try:
					_, _, data_iter = importer.import_file.parse_next_row_for_import_iterator(data_iter)
					count += 1
				except StopIteration:
					break

			self.payload_count = count


	@frappe.whitelist()
	def get_preview_from_template(self, import_file=None, google_sheets_url=None):
		if import_file:
			self.import_file = import_file

		if google_sheets_url:
			self.google_sheets_url = google_sheets_url

		if not (self.import_file or self.google_sheets_url):
			return

		i = self.get_importer()
		return i.get_data_for_import_preview()

	def start_import(self):
		from frappe.utils.scheduler import is_scheduler_inactive

		run_now = frappe.flags.in_test or frappe.conf.developer_mode
		if is_scheduler_inactive() and not run_now:
			frappe.throw(_("Scheduler is inactive. Cannot import data."), title=_("Scheduler Inactive"))

		job_id = f"data_import::{self.name}"

		if not is_job_enqueued(job_id):
			enqueue(
				start_import,
				queue="default",
				timeout=10000,
				event="data_import",
				job_id=job_id,
				data_import=self.name,
				now=run_now,
			)
			return True

		return False

	def export_errored_rows(self):
		return self.get_importer().export_errored_rows()

	def download_import_log(self):
		return self.get_importer().export_import_log()

	def get_importer(self):
		return Importer(self.reference_doctype, data_import=self)

	def on_trash(self):
		frappe.db.delete("Data Import Log", {"data_import": self.name})


@frappe.whitelist()
def get_preview_from_template(
	data_import: str, import_file: str | None = None, google_sheets_url: str | None = None
):
	di: DataImport = frappe.get_doc("Data Import Custom", data_import)
	di.check_permission("read")
	return di.get_preview_from_template(import_file, google_sheets_url)


# @frappe.whitelist()
# def form_start_import(data_import: str):
# 	di: DataImport = frappe.get_doc("Data Import Custom", data_import)
# 	di.check_permission("write")
# 	return di.start_import()


def start_import(data_import):
	"""This method runs in background job"""
	data_import = frappe.get_doc("Data Import Custom", data_import)
	try:
		i = Importer(data_import.reference_doctype, data_import=data_import)
		i.import_data()
	except JobTimeoutException:
		frappe.db.rollback()
		data_import.db_set("status", "Timed Out")
	except Exception:
		frappe.db.rollback()
		data_import.db_set("status", "Error")
		data_import.log_error("Data import failed")
	finally:
		frappe.flags.in_import = False

	frappe.publish_realtime("data_import_refresh", {"data_import": data_import.name})


@frappe.whitelist()
def download_template(doctype, export_fields=None, export_records=None, export_filters=None, file_type="CSV"):
	"""
	Download template from Exporter
	        :param doctype: Document Type
	        :param export_fields=None: Fields to export as dict {'Sales Invoice': ['name', 'customer'], 'Sales Invoice Item': ['item_code']}
	        :param export_records=None: One of 'all', 'by_filter', 'blank_template'
	        :param export_filters: Filter dict
	        :param file_type: File type to export into
	"""
	frappe.has_permission(doctype, "read", throw=True)

	export_fields = frappe.parse_json(export_fields)
	export_filters = frappe.parse_json(export_filters)
	export_data = export_records != "blank_template"

	e = Exporter(
		doctype,
		export_fields=export_fields,
		export_data=export_data,
		export_filters=export_filters,
		file_type=file_type,
		export_page_length=5 if export_records == "5_records" else None,
	)
	e.build_response()


@frappe.whitelist()
def download_errored_template(data_import_name: str):
	data_import: DataImport = frappe.get_doc("Data Import Custom", data_import_name)
	data_import.check_permission("read")
	data_import.export_errored_rows()


@frappe.whitelist()
def download_import_log(data_import_name: str):
	data_import: DataImport = frappe.get_doc("Data Import Custom", data_import_name)
	data_import.check_permission("read")
	data_import.download_import_log()


@frappe.whitelist()
def get_import_status(data_import_name: str):
	data_import: DataImport = frappe.get_doc("Data Import Custom", data_import_name)
	data_import.check_permission("read")

	import_status = {"status": data_import.status}
	logs = frappe.get_all(
		"Data Import Log",
		fields=["count(*) as count", "success"],
		filters={"data_import": data_import_name},
		group_by="success",
	)

	total_payload_count = data_import.payload_count

	for log in logs:
		if log.get("success"):
			import_status["success"] = log.get("count")
		else:
			import_status["failed"] = log.get("count")

	import_status["total_records"] = total_payload_count

	return import_status


# @frappe.whitelist()
# def get_import_logs(data_import: str):
# 	doc = frappe.get_doc("Data Import Custom", data_import)
# 	doc.check_permission("read")

# 	return frappe.get_all(
# 		"Data Import Log",
# 		fields=["success", "docname", "messages", "exception", "row_indexes"],
# 		filters={"data_import": data_import},
# 		limit_page_length=5000,
# 		order_by="log_index",
# 	)


def import_file(doctype, file_path, import_type, submit_after_import=False, console=False):
	"""
	Import documents in from CSV or XLSX using data import.

	:param doctype: DocType to import
	:param file_path: Path to .csv, .xls, or .xlsx file to import
	:param import_type: One of "Insert" or "Update"
	:param submit_after_import: Whether to submit documents after import
	:param console: Set to true if this is to be used from command line. Will print errors or progress to stdout.
	"""

	data_import = frappe.new_doc("Data Import Custom")
	data_import.submit_after_import = submit_after_import
	data_import.import_type = (
		"Insert New Records" if import_type.lower() == "insert" else "Update Existing Records"
	)

	i = Importer(doctype=doctype, file_path=file_path, data_import=data_import, console=console)
	i.import_data()


def import_doc(path, pre_process=None):
	if os.path.isdir(path):
		files = [os.path.join(path, f) for f in os.listdir(path)]
	else:
		files = [path]

	for f in files:
		if f.endswith(".json"):
			frappe.flags.mute_emails = True
			import_file_by_path(
				f, data_import=True, force=True, pre_process=pre_process, reset_permissions=True
			)
			frappe.flags.mute_emails = False
			frappe.db.commit()
		else:
			raise NotImplementedError("Only .json files can be imported")


def export_json(doctype, path, filters=None, or_filters=None, name=None, order_by="creation asc"):
	def post_process(out):
		# Note on Tree DocTypes:
		# The tree structure is maintained in the database via the fields "lft"
		# and "rgt". They are automatically set and kept up-to-date. Importing
		# them would destroy any existing tree structure. For this reason they
		# are not exported as well.
		del_keys = ("modified_by", "creation", "owner", "idx", "lft", "rgt")
		for doc in out:
			for key in del_keys:
				if key in doc:
					del doc[key]
			for v in doc.values():
				if isinstance(v, list):
					for child in v:
						for key in (*del_keys, "docstatus", "doctype", "modified", "name"):
							if key in child:
								del child[key]

	out = []
	if name:
		out.append(frappe.get_doc(doctype, name).as_dict())
	elif frappe.db.get_value("DocType", doctype, "issingle"):
		out.append(frappe.get_doc(doctype).as_dict())
	else:
		for doc in frappe.get_all(
			doctype,
			fields=["name"],
			filters=filters,
			or_filters=or_filters,
			limit_page_length=0,
			order_by=order_by,
		):
			out.append(frappe.get_doc(doctype, doc.name).as_dict())
	post_process(out)

	dirname = os.path.dirname(path)
	if not os.path.exists(dirname):
		path = os.path.join("..", path)

	with open(path, "w") as outfile:
		outfile.write(frappe.as_json(out, ensure_ascii=False))


def export_csv(doctype, path):
	from frappe.core.doctype.data_export.exporter import export_data

	with open(path, "wb") as csvfile:
		export_data(doctype=doctype, all_doctypes=True, template=True, with_data=True)
		csvfile.write(frappe.response.result.encode("utf-8"))

@frappe.whitelist()
def form_start_import(data_import: str, batch_size: int = 25):
	di = frappe.get_doc("Data Import Custom", data_import)
	di.check_permission("write")

	di.db_set("status", "Preprocessing")

	enqueue(
		"custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.process_import_batch",
		queue="long",
		timeout=30000,
		event="data_import",
		data_import=di.name,
		payload_start=0,
		batch_size=batch_size  
	)

	return {"message": "Import job queued, processing in background."}


def process_import_batch(data_import, payload_start: int = 0, batch_size: int = 10):
	di = frappe.get_doc("Data Import Custom", data_import)
	cache = frappe.cache()

	if not cache.get_value(f"data_import:{di.name}:total_payloads"):
		importer = di.get_importer()
		data_iter = iter(importer.import_file.data)
		total_payloads = 0

		while True:
			try:
				_, _, data_iter = importer.import_file.parse_next_row_for_import_iterator(data_iter)
				total_payloads += 1
			except StopIteration:
				break

		if total_payloads == 0:
			di.db_set("status", "Success")
			frappe.publish_realtime("data_import_refresh", {"data_import": di.name})
			return {"message": "No records to import", "total": 0}

		di.db_set("payload_count", total_payloads)

		cache.set_value(f"data_import:{di.name}:total_payloads", total_payloads)
		cache.set_value(f"data_import:{di.name}:batch_size", int(batch_size))
		cache.set_value(f"data_import:{di.name}:processed_payloads", 0)
		cache.set_value(
			f"data_import:{di.name}:total_batches",
			(total_payloads + int(batch_size) - 1) // int(batch_size)
		)
	
	try:

		di.db_set("status", "Running")
		cache_key_payloads = f"data_import:{di.name}:payloads"
		payloads = cache.get_value(cache_key_payloads)

		importer = Importer(di.reference_doctype, data_import=di)  

		if not payloads:
			payloads = importer.import_file.get_payloads_for_import()
			cache.set_value(cache_key_payloads, payloads)


		total_payloads = int(cache.get_value(f"data_import:{di.name}:total_payloads"))
		batch_size = int(cache.get_value(f"data_import:{di.name}:batch_size"))
		total_batches = int(cache.get_value(f"data_import:{di.name}:total_batches"))

		payload_end = min(payload_start + batch_size, total_payloads)

		batch_payloads = payloads[payload_start:payload_end]

		batch_index = (payload_start // batch_size) + 1

		log_index_base = frappe.db.count("Data Import Log", {"data_import": di.name}) or 0

		for payload in batch_payloads:
			
			if cache.get_value(f"data_import:{di.name}:stop"):
				di.db_set("status", "Stopped")
				finalize_import_status(di.name, stopped=True)
				return

			doc = payload.doc
			row_indexes = [r.row_number for r in payload.rows]

			try:
				created_doc = importer.process_doc(doc)

				if di.submit_after_import and created_doc.docstatus == 0:
					created_doc.submit()

				create_import_log(
					di.name,
					log_index_base,
					{
						"success": True,
						"docname": created_doc.name,
						"row_indexes": row_indexes,
					},
				)
				log_index_base += 1
				frappe.db.commit()

			except Exception:
				messages = frappe.local.message_log
				frappe.clear_messages()
				frappe.db.rollback()

				create_import_log(
					di.name,
					log_index_base,
					{
						"success": False,
						"exception": frappe.get_traceback(),
						"messages": messages,
						"row_indexes": row_indexes,
					},
				)
				log_index_base += 1
				frappe.db.commit()

		processed = int(cache.get_value(f"data_import:{di.name}:processed_payloads") or 0)
		processed += len(batch_payloads)
		cache.set_value(f"data_import:{di.name}:processed_payloads", processed)

		frappe.publish_realtime(
			"data_import_progress",
			{
				"current": processed,
				"total": total_payloads,
				"data_import": di.name,
				"batch_index": batch_index,
				"total_batches": total_batches,
				"success": True,
				"eta": 0,
			}
		)

		if payload_end < total_payloads:
			next_payload_start = payload_end
			enqueue(
				"custom_import_app.custom_import_app.doctype.data_import_custom.data_import_custom.process_import_batch",
				queue="long",
				timeout=30000,
				event="data_import",
				data_import=di.name,
				payload_start=next_payload_start,
			)
		else:
			finalize_import_status(di.name)

	except JobTimeoutException:
		frappe.db.rollback()
		frappe.get_doc(
			{
				"doctype": "Data Import Log",
				"log_index": frappe.db.count("Data Import Log", {"data_import": di.name}) or 0,
				"success": 0,
				"data_import": di.name,
				"row_indexes": "[]",
				"messages": "[]",
				"exception": "Batch timed out",
			}
		).db_insert()
		frappe.db.commit()
		finalize_import_status(di.name, timed_out=True)

	except Exception:
		frappe.db.rollback()
		frappe.get_doc(
			{
				"doctype": "Data Import Log",
				"log_index": frappe.db.count("Data Import Log", {"data_import": di.name}) or 0,
				"success": 0,
				"data_import": di.name,
				"row_indexes": "[]",
				"messages": "[]",
				"exception": frappe.get_traceback(),
			}
		).db_insert()
		frappe.db.commit()
		finalize_import_status(di.name, error=True)

@frappe.whitelist()
def stop_import(data_import: str):
	cache = frappe.cache()
	cache.set_value(f"data_import:{data_import}:stop", True)

	frappe.db.set_value("Data Import Custom", data_import, "status", "Stopped")

	frappe.publish_realtime("data_import_progress", {
		"data_import": data_import,
		"status": "Stopped"
	})

	return {"message": "Import Stopped..."}


def finalize_import_status(data_import_name: str, timed_out: bool = False, error: bool = False, stopped: bool = False):

	di = frappe.get_doc("Data Import Custom", data_import_name)
	cache = frappe.cache()

	total_payloads = int(cache.get_value(f"data_import:{di.name}:total_payloads") or 0)
	processed = int(cache.get_value(f"data_import:{di.name}:processed_payloads") or 0)

	logs = frappe.get_all(
		"Data Import Log",
		fields=["count(*) as count", "success"],
		filters={"data_import": di.name},
		group_by="success",
	)

	successes = 0
	failures = 0
	for l in logs:
		if l.get("success"):
			successes = l.get("count")
		else:
			failures = l.get("count")

	if timed_out:
		final_status = "Timed Out"
	elif stopped:
		final_status = "Stopped"
	elif error and failures == total_payloads:
		final_status = "Error"
	elif failures > 0 and successes > 0:
		final_status = "Partial Success"
	elif successes == total_payloads:
		final_status = "Success"
	else:
		final_status = "Partial Success" if successes > 0 else "Error"

	di.db_set("status", final_status)

	cache.set_value(f"data_import:{di.name}:total_payloads", None)
	cache.set_value(f"data_import:{di.name}:batch_size", None)
	cache.set_value(f"data_import:{di.name}:processed_payloads", None)
	cache.set_value(f"data_import:{di.name}:total_batches", None)

	frappe.publish_realtime("data_import_refresh", {"data_import": di.name})


@frappe.whitelist()
def get_import_logs(data_import: str):
    frappe.get_doc("Data Import Custom", data_import).check_permission("read")

    query = """
        SELECT
            success,
            docname,
            messages,
            exception,
            row_indexes
        FROM `tabData Import Log`
        WHERE data_import = %s
        ORDER BY log_index DESC
    """
    logs = frappe.db.sql(query, (data_import,), as_dict=True)

    return logs
