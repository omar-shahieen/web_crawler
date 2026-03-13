import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import Keyboard from "react-simple-keyboard";
import "react-simple-keyboard/build/css/index.css";

import { fetchSearchResults, fetchAutoComplete } from "../api/searchApi.js";
import styles from "./SearchBar.module.css";

const RECENT_KEY = "nexus_recent_searches";
const MAX_RECENT = 8;

function readRecentSearches() {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
  } catch {
    return [];
  }
}

function writeRecentSearches(values) {
  try {
    localStorage.setItem(
      RECENT_KEY,
      JSON.stringify(values.slice(0, MAX_RECENT)),
    );
  } catch {
    // Ignore localStorage write errors in private mode.
  }
}

const hasSpeech =
  typeof window !== "undefined" &&
  !!(window.SpeechRecognition || window.webkitSpeechRecognition);

function SearchBar({ initialValue = "", compact = false }) {
  const [query, setQuery] = useState(initialValue);
  const [keyboardOpen, setKeyboardOpen] = useState(false);
  const [recentSearches, setRecentSearches] = useState(() =>
    readRecentSearches(),
  );
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [serverSuggestions, setServerSuggestions] = useState([]);
  const [suggestionLoading, setSuggestionLoading] = useState(false);
  const [luckyLoading, setLuckyLoading] = useState(false);
  const [listening, setListening] = useState(false);
  const keyboardRef = useRef(null);
  const wrapperRef = useRef(null);
  const navigate = useNavigate();

  useEffect(() => {
    setQuery(initialValue);
    keyboardRef.current?.setInput(initialValue);
  }, [initialValue]);

  useEffect(() => {
    function handlePointerDown(event) {
      if (!wrapperRef.current?.contains(event.target)) {
        setShowSuggestions(false);
      }
    }

    document.addEventListener("pointerdown", handlePointerDown);
    return () => document.removeEventListener("pointerdown", handlePointerDown);
  }, []);

  useEffect(() => {
    if (compact) return;
    const trimmed = query.trim();
    console.debug(
      "[SearchBar] autocomplete effect run, showSuggestions=",
      showSuggestions,
      "trimmed=",
      trimmed,
    );
    if (!showSuggestions) {
      console.debug(
        "[SearchBar] not showing suggestions - skipping autocomplete",
      );
      setServerSuggestions([]);
      setSuggestionLoading(false);
      return;
    }

    if (!trimmed) {
      setServerSuggestions([]);
      setSuggestionLoading(false);
      return;
    }

    let cancelled = false;
    const id = setTimeout(async () => {
      console.debug("[SearchBar] debounced fetchAutoComplete for:", trimmed);
      setSuggestionLoading(true);
      try {
        const results = await fetchAutoComplete(trimmed);
        console.debug("[SearchBar] fetchAutoComplete results:", results);
        if (!cancelled) {
          setServerSuggestions(Array.isArray(results) ? results : []);
        }
      } catch (err) {
        console.debug("[SearchBar] fetchAutoComplete error:", err);
        if (!cancelled) setServerSuggestions([]);
      } finally {
        if (!cancelled) setSuggestionLoading(false);
      }
    }, 200);

    return () => {
      cancelled = true;
      clearTimeout(id);
    };
  }, [query, showSuggestions, compact]);

  const filteredSuggestions = useMemo(() => {
    const trimmed = query.trim().toLowerCase();
    if (!trimmed) {
      return recentSearches.slice(0, 5);
    }

    const recentMatches = recentSearches.filter((item) =>
      item.toLowerCase().includes(trimmed),
    );

    const combined = [
      ...(Array.isArray(serverSuggestions) ? serverSuggestions : []),
      ...recentMatches,
    ];

    const unique = Array.from(new Set(combined));
    return unique.slice(0, 5);
  }, [query, recentSearches, serverSuggestions]);

  function persistSearch(term) {
    const next = [term, ...recentSearches.filter((item) => item !== term)];
    setRecentSearches(next.slice(0, MAX_RECENT));
    writeRecentSearches(next);
  }

  function removeRecentSearch(term) {
    const next = recentSearches.filter((item) => item !== term);
    setRecentSearches(next);
    writeRecentSearches(next);
  }

  function navigateToResults(term) {
    const params = new URLSearchParams({ search: term });
    navigate(`/results?${params}`);
  }

  function submitSearch(term) {
    const trimmed = term.trim();
    console.debug("[SearchBar] submitSearch:", trimmed);
    if (!trimmed) return;
    persistSearch(trimmed);
    navigateToResults(trimmed);
  }

  async function handleLuckySearch() {
    const trimmed = query.trim();
    if (!trimmed || luckyLoading) return;

    setLuckyLoading(true);
    persistSearch(trimmed);

    try {
      const results = await fetchSearchResults(trimmed);
      const firstUrl = results?.[0]?.url;
      if (firstUrl) {
        window.location.assign(firstUrl);
      } else {
        navigateToResults(trimmed);
      }
    } catch {
      navigateToResults(trimmed);
    } finally {
      setLuckyLoading(false);
    }
  }

  function handleInputChange(event) {
    const value = event.target.value;
    console.debug("[SearchBar] handleInputChange value:", value);
    setQuery(value);
    keyboardRef.current?.setInput(value);
    if (!compact) {
      setShowSuggestions(true);
    }
  }

  function handleSuggestionClick(value) {
    setQuery(value);
    keyboardRef.current?.setInput(value);
    setShowSuggestions(false);
    submitSearch(value);
  }

  function handleVirtualKeyChange(input) {
    setQuery(input);
    if (!compact) {
      setShowSuggestions(true);
    }
  }

  function handleVirtualKeyPress(button) {
    if (button === "{enter}") {
      submitSearch(query);
      setKeyboardOpen(false);
      setShowSuggestions(false);
    }
  }

  function startVoiceSearch() {
    if (!hasSpeech || listening) return;
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    const recognition = new SR();
    recognition.lang = "en-US";
    recognition.interimResults = false;
    recognition.maxAlternatives = 1;
    setListening(true);
    recognition.onresult = (e) => {
      const transcript = e.results[0][0].transcript;
      setQuery(transcript);
      keyboardRef.current?.setInput(transcript);
      setListening(false);
      submitSearch(transcript);
    };
    recognition.onerror = () => setListening(false);
    recognition.onend = () => setListening(false);
    recognition.start();
  }

  function handleSubmit(event) {
    event.preventDefault();
    submitSearch(query);
    setShowSuggestions(false);
  }

  return (
    <div className={styles.wrapper} ref={wrapperRef}>
      <form
        className={`${styles.form} ${compact ? styles.compact : ""}`}
        onSubmit={handleSubmit}
        role="search"
      >
        <span className={styles.icon} aria-hidden="true">
          <svg
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
        </span>

        <input
          className={styles.input}
          type="search"
          value={query}
          onChange={handleInputChange}
          onFocus={() => !compact && setShowSuggestions(true)}
          placeholder="Search anything…"
          aria-label="Search query"
          autoComplete="off"
          autoFocus={!compact}
        />

        <button
          className={styles.keyboardButton}
          type="button"
          aria-label={
            keyboardOpen ? "Hide virtual keyboard" : "Show virtual keyboard"
          }
          onClick={() => setKeyboardOpen((open) => !open)}
        >
          ⌨
        </button>

        {hasSpeech && (
          <button
            className={`${styles.micButton} ${listening ? styles.listening : ""}`}
            type="button"
            aria-label={listening ? "Listening…" : "Search by voice"}
            onClick={startVoiceSearch}
            disabled={listening}
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 1a4 4 0 0 1 4 4v6a4 4 0 0 1-8 0V5a4 4 0 0 1 4-4zm-7 10a7 7 0 0 0 14 0h-2a5 5 0 0 1-10 0H5zm7 10v-2a7 7 0 0 1-7-7H3a9 9 0 0 0 8 8.94V21h-3v2h8v-2h-3z" />
            </svg>
          </button>
        )}

        <button
          className={styles.button}
          type="submit"
          aria-label="Submit search"
        >
          Search
        </button>
      </form>

      {!compact && showSuggestions && filteredSuggestions.length > 0 && (
        <ul
          className={styles.suggestionList}
          role="listbox"
          aria-label="Recent search suggestions"
        >
          {filteredSuggestions.map((item) => (
            <li key={item} className={styles.suggestionRow}>
              <button
                className={styles.suggestionItem}
                type="button"
                onClick={() => handleSuggestionClick(item)}
              >
                <span className={styles.suggestionIcon} aria-hidden="true">
                  ↻
                </span>
                {item}
              </button>

              <button
                className={styles.removeSuggestionButton}
                type="button"
                aria-label={`Remove ${item} from recent searches`}
                title="Remove from recent searches"
                onClick={(event) => {
                  event.stopPropagation();
                  removeRecentSearch(item);
                }}
              >
                x
              </button>
            </li>
          ))}
        </ul>
      )}

      {!compact && (
        <div className={styles.actionRow}>
          <button
            className={styles.subtleButton}
            type="button"
            onClick={handleLuckySearch}
            disabled={!query.trim() || luckyLoading}
          >
            {luckyLoading ? "Loading…" : "I'm Feeling Lucky"}
          </button>
        </div>
      )}

      {keyboardOpen && (
        <div className={styles.keyboardPanel}>
          <Keyboard
            keyboardRef={(ref) => {
              keyboardRef.current = ref;
            }}
            inputName="main"
            onChange={handleVirtualKeyChange}
            onKeyPress={handleVirtualKeyPress}
            layout={{
              default: [
                "1 2 3 4 5 6 7 8 9 0 {bksp}",
                "q w e r t y u i o p",
                "a s d f g h j k l",
                "z x c v b n m",
                "{space} {enter}",
              ],
            }}
            display={{
              "{bksp}": "⌫",
              "{space}": "Space",
              "{enter}": "Search",
            }}
          />
        </div>
      )}
    </div>
  );
}

export default SearchBar;
