<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE xsl:stylesheet  [
	<!ENTITY nbsp   "&#160;">
	<!ENTITY copy   "&#169;">
	<!ENTITY reg    "&#174;">
	<!ENTITY trade  "&#8482;">
	<!ENTITY mdash  "&#8212;">
	<!ENTITY ldquo  "&#8220;">
	<!ENTITY rdquo  "&#8221;"> 
	<!ENTITY pound  "&#163;">
	<!ENTITY yen    "&#165;">
	<!ENTITY euro   "&#8364;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="html" encoding="UTF-8" doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN" doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd" indent="yes" version="4.0"/>
<xsl:template match="/">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS" />
<meta name="Keywords" content="Indelible" />
<meta name="Description" content="iGeek Indelible File Sharing" />
<meta name="robots" content="index, follow" />
<meta name="Author" content="iGeek.Inc" />
<meta http-equiv="refresh" content="2"/>
<title>iGeek.Inc</title>
</head>  
<body>
	<table width="600" align="center">
		<tr>
			<td><img src="/assets/NewLogo.jpg" height="80"/></td>
			<td><img src="/assets/indelible_server_icon_cloud_256.png" height="80"/></td>
		</tr>
	</table>
	<table align="center">
		<tr>
			<td style="padding-left: 30px; padding-right: 30px"><div style="font: 45pt Arial">Indelible FS Virtual Disk</div></td>
		</tr>
	</table>
	<div style="font: 20pt Arial">
		<xsl:apply-templates/>
		<table width="600" align="center">
			<tr>
				<td><img src="/assets/wiki.png" height="80"/></td>
				<td></td>
			</tr>
		</table>
	</div>
</body>
</html>
</xsl:template>
<xsl:template match="performance">
	<table align="center">
		<tr>
			<td style="padding-left:10 px; padding-right: 10px; text-align:center">SCSI Target</td>
			<td style="padding-left:10 px; padding-right: 10px; text-align:center">Backing Path</td>
			<td>State</td>
			<td>Size</td>
			<td style="padding-left:10 px; padding-right: 10px; text-align:center">Read Perf</td>
			<td style="padding-left:10 px; padding-right: 10px; text-align:center">Write Perf</td></tr>
		<xsl:for-each select="target">
			<tr>
				<td style="padding-left:10 px; padding-right: 10px; text-align:center"><xsl:value-of select="targetName"/></td>
				<td style="padding-left:10 px; padding-right: 10px; text-align:left"><xsl:value-of select="targetVolumePath"/></td>
				<td style="padding-left:10 px; padding-right: 10px; text-align:left"><xsl:value-of select="state"/></td>
				<td style="padding-left:10 px; padding-right: 10px; text-align:center"><xsl:value-of select="size"/></td>
				<td style="padding-left:10 px; padding-right: 10px; text-align:center"><xsl:value-of select="readBytesPerSecond"/></td>
				<td style="padding-left:10 px; padding-right: 10px; text-align:center"><xsl:value-of select="writeBytesPerSecond"/></td>
			</tr>
		</xsl:for-each>
	</table>
</xsl:template>
</xsl:stylesheet>