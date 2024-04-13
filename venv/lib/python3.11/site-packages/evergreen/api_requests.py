"""Objects for making requests to the API."""
from typing import Any, Dict, List, NamedTuple, Optional

from pydantic import BaseModel


class IssueLinkRequest(NamedTuple):
    """Issue to add to a task annotation."""

    issue_key: str
    url: str
    confidence_score: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        """Get a dictionary representation of the issue link."""
        data: Dict[str, Any] = {"issue_key": self.issue_key, "url": self.url}
        if self.confidence_score is not None:
            data["confidence_score"] = self.confidence_score
        return data


class MetadataLinkRequest(NamedTuple):
    """Metadata Link to add to a task annotation."""

    url: str
    text: str


class SlackAttachmentField(BaseModel):
    """
    Slack fields that get displayed in a table-like format.

    title: The field title.
    value: The field text. It can be formatted as plain text or with markdown by using mrkdwn_in.
    short: Indicates whether the field object is short enough to be displayed side-by-side with
    other field objects.
    """

    title: Optional[str]
    value: Optional[str]
    short: Optional[bool]


class SlackAttachment(BaseModel):
    """
    An attachment to be sent using Slack.

    title: The attachment title.
    title_link: A URL that turns the title into a link.
    text: If fallback is empty, this is required. The main body text of the attachment as plain
    text, or with markdown using mrkdwn_in.
    fallback: If text is empty, this is required. A plain text summary of an attachment for
    clients that don't show formatted text (eg. IRC, mobile notifications).
    mrkdwn_in: An array of fields that should be formatted with markdown.
    color: The message color. Can either be one of good (green), warning (yellow), danger (red),
    or any hex color code (eg. #439FE0).
    author_name: The display name of the author.
    author_icon: A URL that displays the author icon. Will only work if author_name is present.
    fields: Array of SlackAttachmentFields that get displayed in a table-like format.
    """

    title: Optional[str] = None
    title_link: Optional[str] = None
    text: Optional[str] = None
    fallback: Optional[str] = None
    mrkdwn_in: Optional[List[str]] = None
    color: Optional[str] = None
    author_name: Optional[str] = None
    author_icon: Optional[str] = None
    fields: Optional[List[SlackAttachmentField]] = None
