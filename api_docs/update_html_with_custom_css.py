# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

from bs4 import BeautifulSoup
import cssutils
from cssutils.css import CSSStyleSheet, CSSStyleRule, CSSMediaRule
import itertools
import logging

cssutils.ser.prefs.useMinified()

# Silence logging warnings
logger = logging.getLogger("CSSUTILS")
logger.setLevel(logging.CRITICAL)

# Custom CSS (To update styles unavailable in the Community Edition of ReDoc)
custom_css = cssutils.parseFile("customstyles.css")


def get_custom_css_updates(custom_css: CSSStyleSheet) -> CSSStyleSheet.cssRules:
    custom_css_updates: CSSStyleSheet.cssRules = []
    for rule in custom_css.cssRules:
        custom_css_updates.append(rule)
    return custom_css_updates


def _is_media_rule(x: CSSMediaRule) -> bool:
    return x.type == x.MEDIA_RULE


def _is_style_rule(x: CSSStyleRule) -> bool:
    return x.type == x.STYLE_RULE


def maybe_update_style_rule(rule_html: CSSStyleRule, rule_custom_css: CSSStyleRule) -> bool:
    updated = False
    if rule_html.selectorText == rule_custom_css.selectorText:
        for property in rule_html.style:
            for property_custom in rule_custom_css.style:
                # Overwrite any differences in properties for this rule
                if property.name == property_custom.name and property.value != property_custom.value:
                    updated = True
                    before = rule_html.style[property_custom.name]
                    rule_html.style[property_custom.name] = property_custom.value
                    after = rule_html.style[property_custom.name]
                    print(
                        f"Modified:\n"
                        f"\tselector: {rule_html.selectorText},\n"
                        f"\tproperty: '{property_custom.name}' from {before} to {after}",
                        flush=True,
                    )
    return updated


def update_all_styles(rules_old: CSSStyleRule, rules_new: CSSStyleRule):
    updated = False
    for rule_old, rule_new in itertools.product(rules_old, rules_new):
        if _is_style_rule(rule_old) and _is_style_rule(rule_new):
            if maybe_update_style_rule(rule_old, rule_new):
                updated = True

        # Recurse to update all matching STYLE_RULEs for each matching MEDIA_RULE
        if _is_media_rule(rule_old) and _is_media_rule(rule_new):
            old_media_rule: CSSMediaRule = rule_old
            new_media_rule: CSSMediaRule = rule_new
            if old_media_rule.media.mediaText == new_media_rule.media.mediaText:
                if update_all_styles(old_media_rule.cssRules, new_media_rule.cssRules):
                    updated = True
    return updated


def update_html_style_sheet(
    sheet: CSSStyleSheet, custom_css_updates: CSSStyleSheet.cssRules
) -> tuple[CSSStyleSheet, bool]:
    """property_updates are a typle of (rule, property) string pairs"""
    updated = update_all_styles(sheet.cssRules, custom_css_updates)
    return sheet, updated


# Redoc bundle (with Community Edition theme options modified)
with open("redoc-static.html", "rb") as f:
    redoc_html = f.read().decode("utf-8")
    input_html = BeautifulSoup(redoc_html, "html.parser")

# Do the update of all style tags
custom_css_updates = get_custom_css_updates(custom_css)

# There is one special case where cssutils loses relevant data when writing back to string
BAD_TRANSLATION = ".bBhNDy{background:#263238;position:absolute;top:0;bottom:0;right:0}"
CORRECT_TRANSLATION = (
    ".bBhNDy{background:#263238;position:absolute;top:0;bottom:0;right:0;width:calc((100% - 260px) * 0.4);}"
)

for style in input_html.head.find_all_next("style"):
    sheet = cssutils.parseString(style.text)
    new_html_style, updated = update_html_style_sheet(sheet, custom_css_updates)
    if updated:
        new_tag = input_html.new_tag("style", attrs=style.attrs)

        translated_string = new_html_style.cssText.decode("utf-8")
        correct_translation = translated_string.replace(BAD_TRANSLATION, CORRECT_TRANSLATION)

        new_tag.string = correct_translation
        style.replace_with(new_tag)

# Write out modified html
with open("redoc-static.html", "wb") as f:
    f.write(str(input_html).encode("utf-8"))
