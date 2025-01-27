% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/SOMAArrayBase.R
\name{SOMAArrayBase}
\alias{SOMAArrayBase}
\title{SOMA Array Base Class}
\description{
Adds SOMA-specific functionality to the \code{\link{TileDBArray}} class.  (lifecycle: experimental)
}
\section{Super classes}{
\code{\link[tiledbsoma:TileDBObject]{tiledbsoma::TileDBObject}} -> \code{\link[tiledbsoma:TileDBArray]{tiledbsoma::TileDBArray}} -> \code{SOMAArrayBase}
}
\section{Active bindings}{
\if{html}{\out{<div class="r6-active-bindings">}}
\describe{
\item{\code{soma_type}}{Retrieve the SOMA object type.}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-SOMAArrayBase-read_complete}{\code{SOMAArrayBase$read_complete()}}
\item \href{#method-SOMAArrayBase-read_next}{\code{SOMAArrayBase$read_next()}}
\item \href{#method-SOMAArrayBase-clone}{\code{SOMAArrayBase$clone()}}
}
}
\if{html}{\out{
<details><summary>Inherited methods</summary>
<ul>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBObject" data-id="class"><a href='../../tiledbsoma/html/TileDBObject.html#method-TileDBObject-class'><code>tiledbsoma::TileDBObject$class()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBObject" data-id="exists"><a href='../../tiledbsoma/html/TileDBObject.html#method-TileDBObject-exists'><code>tiledbsoma::TileDBObject$exists()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBObject" data-id="initialize"><a href='../../tiledbsoma/html/TileDBObject.html#method-TileDBObject-initialize'><code>tiledbsoma::TileDBObject$initialize()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="attributes"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-attributes'><code>tiledbsoma::TileDBArray$attributes()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="attrnames"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-attrnames'><code>tiledbsoma::TileDBArray$attrnames()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="dimensions"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-dimensions'><code>tiledbsoma::TileDBArray$dimensions()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="dimnames"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-dimnames'><code>tiledbsoma::TileDBArray$dimnames()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="fragment_count"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-fragment_count'><code>tiledbsoma::TileDBArray$fragment_count()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="get_metadata"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-get_metadata'><code>tiledbsoma::TileDBArray$get_metadata()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="print"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-print'><code>tiledbsoma::TileDBArray$print()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="reset_query"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-reset_query'><code>tiledbsoma::TileDBArray$reset_query()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="schema"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-schema'><code>tiledbsoma::TileDBArray$schema()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="set_metadata"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-set_metadata'><code>tiledbsoma::TileDBArray$set_metadata()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="set_query"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-set_query'><code>tiledbsoma::TileDBArray$set_query()</code></a></span></li>
<li><span class="pkg-link" data-pkg="tiledbsoma" data-topic="TileDBArray" data-id="tiledb_array"><a href='../../tiledbsoma/html/TileDBArray.html#method-TileDBArray-tiledb_array'><code>tiledbsoma::TileDBArray$tiledb_array()</code></a></span></li>
</ul>
</details>
}}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-SOMAArrayBase-read_complete"></a>}}
\if{latex}{\out{\hypertarget{method-SOMAArrayBase-read_complete}{}}}
\subsection{Method \code{read_complete()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{SOMAArrayBase$read_complete()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-SOMAArrayBase-read_next"></a>}}
\if{latex}{\out{\hypertarget{method-SOMAArrayBase-read_next}{}}}
\subsection{Method \code{read_next()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{SOMAArrayBase$read_next()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-SOMAArrayBase-clone"></a>}}
\if{latex}{\out{\hypertarget{method-SOMAArrayBase-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{SOMAArrayBase$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
